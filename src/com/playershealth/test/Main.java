package com.playershealth.test;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;


/**
 * Решение тестового задания.
 *
 * @author Dmitrii Goncharov
 */
public class Main {
    /**
     * Индекс идентификатора в строке.
     */
    private static final int ID_INDEX = 0;
    /**
     * Индекс цены в строке.
     */
    private static final int PRICE_INDEX = 4;
    /**
     * Количество потоков.
     */
    private static final int THREADS = 6;
    /**
     * Максимальное количество записей для продуктов с одинаковым ID.
     */
    private static final int MAX_COUNT_FOR_SOME_PRODUCT = 20;
    /**
     * Количество продуктов в итоговой выборке.
     */
    private static final AtomicInteger maxProductCount = new AtomicInteger(1000);
    /**
     * Директория с исходными файлами.
     */
    private static final String INPUT_DIR = "/home/user/data";
    /**
     * Файл для сохранения результата.
     */
    private static final String OUTPUT_FILE_PATH = "/home/user/result.csv";
    /**
     * Коллекция с минимальными ценами.
     */
    private static final PriorityBlockingQueue<Float> minPrices =
        new PriorityBlockingQueue<>(maxProductCount.get() + 1, new FloatComparator().reversed());
    /**
     * Карта соответствия ID и набора продуктов с минимальной ценой.
     */
    private static final Map<Integer, PriorityQueue<String[]>> map = new HashMap<>();
    /**
     * Mutex.
     */
    private static final Object mutex = new Object();


    /**
     * Запускающий метод.
     *
     * @param args аргументы командной строки
     * @throws IOException при ошибках ввода/вывода
     */
    public static void main(String[] args) throws IOException {
        try (Stream<Path> paths = Files.walk(Paths.get(INPUT_DIR))) {
            final ExecutorService executorService = Executors.newFixedThreadPool(THREADS);
            paths
                .filter(Files::isRegularFile)
                .filter(file -> file.getFileName().toString().endsWith(".csv"))
                .forEach(path -> executorService.execute(() -> {
                    try (final RandomAccessFile file = new RandomAccessFile(path.toFile(), "r")) {
                        boolean flag = true;
                        while (flag) {
                            final String line = file.readLine();
                            if (line == null) {
                                flag = false;
                            } else {
                                processLine(line);
                            }
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }));
            executorService.shutdown();
            while (!executorService.isTerminated()) {
            }
        }

        final ArrayList<String[]> result = new ArrayList<>();
        map.values().forEach(result::addAll);
        result.sort(new ProductComparator());
        try (RandomAccessFile file = new RandomAccessFile(OUTPUT_FILE_PATH, "rw")) {
            final Iterator<String[]> iterator = result.iterator();
            while (iterator.hasNext()) {
                String s = String.join(";", iterator.next()) + System.lineSeparator();
                file.write(s.getBytes(StandardCharsets.UTF_8));
            }
        }
    }

    /**
     * Обрабатывает одну строку в файле.
     *
     * @param line строка
     */
    private static void processLine(String line) {
        final String[] product = line.split(";");
        if (product.length != 5) {
            System.out.println("Неверный формат записи: " + product);
        } else {
            // Первоначальное заполнение до максимума
            if (maxProductCount.get() > 0) {
                synchronized (mutex) {
                    if (maxProductCount.get() > 0) {
                        maxProductCount.decrementAndGet();
                        simplyAddElement(product);
                    } else {
                        addProduct(product);
                    }
                }
            } else {
                addProduct(product);
            }
        }
    }

    /**
     * Добавляет продукт во временные коллекции.
     *
     * @param product продукт
     */
    private static void addProduct(String[] product) {
        final int id = Integer.valueOf(product[ID_INDEX]);
        final float price = Float.valueOf(product[PRICE_INDEX]);
        if (minPrices.peek() > price) {
            synchronized (map) {
                if (!map.containsKey(id)) {
                    putNewProduct(id, price, product);
                    removeMaxPrice();
                } else if (map.get(id).size() < MAX_COUNT_FOR_SOME_PRODUCT) {
                    addProduct(id, price, product);
                    removeMaxPrice();
                } else if (Float.valueOf(map.get(id).peek()[PRICE_INDEX]) > price) {
                    map.get(id).add(product);
                    final String[] strings = map.get(id).poll();
                    minPrices.remove(Float.valueOf(strings[PRICE_INDEX]));
                    minPrices.put(price);
                }
            }
        }
    }

    /**
     * Удаляет максимальную цену из коллекций.
     */
    private static void removeMaxPrice() {
        map.values().stream()
            .filter(strings -> !strings.isEmpty())
            .map(PriorityQueue::peek)
            .max(new ProductComparator())
            .ifPresent(arr -> {
                map.get(Integer.valueOf(arr[ID_INDEX])).poll();
                minPrices.remove(Float.valueOf(arr[PRICE_INDEX]));
            });
    }

    /**
     * Добавление продукта в коллекции, если продукт с таким ИД уже встречался.
     *
     * @param id      ИД продукта
     * @param price   цена продукта
     * @param product продукт
     */
    private static void addProduct(int id, float price, String[] product) {
        map.get(id).add(product);
        minPrices.put(price);
    }

    /**
     * Добавление нового продукта в коллекции.
     *
     * @param id      ИД продукта
     * @param price   цена продукта
     * @param product продукт
     */
    private static void putNewProduct(int id, float price, String[] product) {
        final PriorityQueue<String[]> sortedProducts = new PriorityQueue<>(new ProductComparator().reversed());
        sortedProducts.add(product);
        map.put(id, sortedProducts);
        minPrices.put(price);
    }

    /**
     * Добавление продукта при первоначальном заполнении.
     *
     * @param product продукт
     */
    private static void simplyAddElement(String[] product) {
        final int id = Integer.valueOf(product[ID_INDEX]);
        final float price = Float.valueOf(product[PRICE_INDEX]);
        if (!map.containsKey(id)) {
            putNewProduct(id, price, product);
        } else if (map.get(id).size() < MAX_COUNT_FOR_SOME_PRODUCT) {
            addProduct(id, price, product);
        } else if (Float.valueOf(map.get(id).peek()[PRICE_INDEX]) > price) {
            map.get(id).add(product);
            final String[] strings = map.get(id).poll();
            minPrices.remove(Float.valueOf(strings[PRICE_INDEX]));
            maxProductCount.incrementAndGet();
        } else {
            maxProductCount.incrementAndGet();
        }

    }

    /**
     * {@link Comparator} для продуктов.
     */
    static class ProductComparator implements Comparator<String[]> {
        @Override
        public int compare(String[] o1, String[] o2) {
            final float f1 = Float.parseFloat(o1[PRICE_INDEX]);
            final float f2 = Float.parseFloat(o2[PRICE_INDEX]);
            return Float.compare(f1, f2);
        }
    }

    /**
     * {@link Comparator} для {@link Float}.
     * Нужен для передачи в коллекцию в развернутом виде.
     */
    static class FloatComparator implements Comparator<Float> {

        @Override
        public int compare(Float o1, Float o2) {

            return Float.compare(o1, o2);
        }
    }
}

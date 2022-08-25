import numpy as np
#1. Дан случайный массив, поменять знак у элементов, значения которых между 3 и 8

ex_array = np.random.randint(1,10,5)

ex_array[(ex_array >3) & (ex_array <8)] *= -1
print(ex_array)

#2. Заменить максимальный элемент случайного массива на 0

ex_array[ex_array.argmax()] = 0
print(ex_array)


#3. Построить прямое произведение массивов (все комбинации с каждым элементом). На вход подается двумерный массив

def cartesian_product(array):
    array = [np.asarray(a) for a in array]
    ar_shape = map(len, array)
    ix = np.indices(ar_shape, dtype=int)
    ix = ix.reshape(len(array), -1).T

    for n, arr in enumerate(array):
        ix[:, n] = array[n][ix[:, n]]
    return ix

print(cartesian_product(([7,6,2], [1,3,5])))

#4. Даны 2 массива A (8x3) и B (2x2). Найти строки в A, которые содержат элементы из каждой строки в B, независимо от
# порядка элементов в B

A = np.random.randint(0,10,(8,3))
B = np.random.randint(0,10,(2,2))
C = (A[..., np.newaxis, np.newaxis] == B )
result = (C.sum(axis=(1,2,3)) >= B.shape[1]).nonzero()[0]
print(result)

#5. Дана 10x3 матрица, найти строки из неравных значений (например строка [2,2,3] остается, строка [3,3,3] удаляется)

A = np.random.randint(0,10,(10,3))
B = np.logical_and.reduce(A[:,1:] == A[:,:-1], axis=1)
result = A[~B]
print(result)

#6. Дан двумерный массив. Удалить те строки, которые повторяются

ex_array = np.array([[1,2,3],
                     [1,2,3],
                     [0,1,2]])
new_array = np.unique(ex_array,axis = 0)
print(new_array)

#Для каждой из следующих задач (1-5) нужно привести 2 реализации – одна без использования numpy (cчитайте, что там, где
# на входе или выходе должны быть numpy array, будут просто списки), а вторая полностью векторизованная с использованием
# numpy (без использования питоновских циклов/map/list comprehension).
#Замечание 1. Можно считать, что все указанные объекты непустые (к примеру, в задаче 1 на диагонали матрицы есть
# ненулевые элементы).
#Замечание 2. Для большинства задач решение занимает не больше 1-2 строк.

#Задача 1: Подсчитать произведение ненулевых элементов на диагонали прямоугольной матрицы.
#Например, для X = np.array([[1, 0, 1], [2, 0, 2], [3, 0, 3], [4, 4, 4]]) ответ 3.
# with numpy
x = np.array([[1, 0, 1], [2, 0, 2], [3, 0, 3], [4, 4, 4]])
diag = np.diag(x)
prod = diag[diag != 0].prod()
print(prod)

#without numpy
x = np.array([[1, 0, 1], [2, 0, 2], [3, 0, 3], [4, 4, 4]])
diag_len = min(len(x), len(x[0]))
prod = 1
for i in range(diag_len):
    if(x[i][i] != 0):
        prod *= x[i][i]
print(prod)

#Задача 2: Даны два вектора x и y. Проверить, задают ли они одно и то же мультимножество.
#Например, для x = np.array([1, 2, 2, 4]), y = np.array([4, 2, 1, 2]) ответ True.
# with numpy
def equal_multisets(x, y):
    x_nums, x_counts = np.unique(x, return_counts=True)
    y_nums, y_counts = np.unique(y, return_counts=True)
    if(np.shape(x_nums) != np.shape(y_nums)):
        return False
    if(np.any(x_nums != y_nums) or np.any(x_counts != y_counts)):
        return False
    return True
print(equal_multisets(([1, 2, 2, 4]), ([4, 2, 1, 2])))

#without numpy
def equal_multisets_wo_np(x, y):
    x.sort()
    y.sort()
    if(len(x) != len(y)):
        return False
    for i in range(len(x)):
        if(x[i] != y[i]):
            return False
    return True
print(equal_multisets(([1, 2, 2, 4]), ([4, 2, 1, 2])))

#Задача 3: Найти максимальный элемент в векторе x среди элементов, перед которыми стоит ноль. Например, для
# x = np.array([6, 2, 0, 3, 0, 0, 5, 7, 0]) ответ 5.
#with np
x = np.array([6, 2, 0, 3, 0, 0, 5, 7, 0])
ad_array = np.ones((1))
ad_array = np.hstack((ad_array, x))
ind = ad_array == 0
result = np.max(x[ind[:-1]])
print(result)

#without np
#def max_after_zero_non_vect(x):
x = np.array([6, 2, 0, 3, 0, 0, 5, 7, 0])
after_zero_list = []
for i in range(1, len(x)):
    if(not x[i - 1]):
        after_zero_list.append(x[i])
print(max(after_zero_list))

#Задача 4: Реализовать кодирование длин серий (Run-length encoding). Для некоторого вектора x необходимо вернуть кортеж
# из двух векторов одинаковой длины. Первый содержит числа, а второй - сколько раз их нужно повторить.
#Например, для x = np.array([2, 2, 2, 3, 3, 3, 5]) ответ (np.array([2, 3, 5]), np.array([3, 3, 1])).
#with np
def run_length_encoding(x):
    ad_array = np.ones(1)
    ad_array = np.hstack((ad_array, x[:- 1]))
    first_positions = x != ad_array
    first_positions[0] = True
    indexes_1 = np.arange(np.size(x))[first_positions]
    indexes_2 = np.hstack((indexes_1[1:], np.array([np.size(x)])))
    return np.array(x)[first_positions], indexes_2 - indexes_1

print(run_length_encoding(([2, 2, 2, 3, 3, 3, 5])))

#without np
def run_length_encoding_wo_np(x):
    val_list = [x[0]]
    num_list = list()
    counter = 1
    for i in range(1, len(x)):
        if(x[i - 1] == x[i]):
            counter = counter + 1
        else:
            val_list.append(x[i])
            num_list.append(counter)
            counter = 1
    num_list.append(counter)
    return (val_list, num_list)

print(run_length_encoding_wo_np(([2, 2, 2, 3, 3, 3, 5])))

#Задача 5: Даны две выборки объектов - X и Y. Вычислить матрицу евклидовых расстояний между объектами. Сравните с
# функцией scipy.spatial.distance.cdist по скорости работы.
#with np
x = np.random.randint(10, size=(2, 2))
y = np.random.randint(10, size=(2, 2))
P = np.add.outer(np.sum(np.power(x,2), axis=1), np.sum(np.power(y,2), axis=1))
N = np.dot(x, y.T)
print(np.sqrt(P - 2*N))

#without np
import math

result_matr = list()
for i in range(len(x)):
    curr_list = list()
    for j in range(len(y)):
        dist = 0
        for k in range(len(x[0])):
            dist += (x[i][k] - y[j][k]) ** 2
        curr_list.append(math.sqrt(dist))
    result_matr.append(curr_list)
print(result_matr)



#### Задача 6: CrunchieMunchies *
# Вы работаете в отделе маркетинга пищевой компании MyCrunch, которая разрабатывает новый вид вкусных, полезных злаков
# под названием CrunchieMunchies.
#
#Вы хотите продемонстрировать потребителям, насколько полезны ваши хлопья по сравнению с другими ведущими брендами,
# поэтому вы собрали данные о питании нескольких разных конкурентов.
#
# Ваша задача - использовать вычисления Numpy для анализа этих данных и доказать, что ваши СrunchieMunchies - самый
# здоровый выбор для потребителей.

calorie_stats = np.loadtxt("./cereal.csv", delimiter=",")
print(calorie_stats)

#В одной порции CrunchieMunchies содержится 60 калорий. Насколько выше среднее количество калорий у ваших конкурентов?
#Сохраните ответ в переменной average_calories и распечатайте переменную в терминале

average_calories = round(np.average(np.array(calorie_stats)) - 60,2)
print(average_calories)

# Корректно ли среднее количество калорий отражает распределение набора данных? Давайте отсортируем данные и посмотрим.
# Отсортируйте данные и сохраните результат в переменной calorie_stats_sorted. Распечатайте отсортированную информацию

calorie_stats_sorted = np.sort(calorie_stats)
print(calorie_stats_sorted)

#Похоже, что большинство значений выше среднего. Давайте посмотрим, является ли медиана наиболее корректным
# показателем набора данных.Вычислите медиану набора данных и сохраните свой ответ в median_calories. Выведите медиану,
# чтобы вы могли видеть, как она сравнивается со средним значением.

median_calories = np.median(calorie_stats)
print(median_calories)

#В то время как медиана показывает, что по крайней мере половина наших значений составляет более 100 калорий, было бы
# более впечатляюще показать, что значительная часть конкурентов имеет более высокое количество калорий, чем
# CrunchieMunchies.Рассчитайте различные процентили и распечатайте их, пока не найдете наименьший процентиль,
# превышающий 60 калорий. Сохраните это значение в переменной nth_percentile.

nth_percentile_2 = np.percentile(calorie_stats,2)
nth_percentile_3 = np.percentile(calorie_stats,3)
nth_percentile_3_3 = np.percentile(calorie_stats,3.3)
nth_percentile = np.percentile(calorie_stats,3.3)
print(nth_percentile)

#Хотя процентиль показывает нам, что у большинства конкурентов количество калорий намного выше, это неудобная концепция
# для использования в маркетинговых материалах.Вместо этого давайте подсчитаем процент хлопьев, в которых содержится
# более 60 калорий на порцию. Сохраните свой ответ в переменной more_calories и распечатайте его
more_than_60 = []
for i in calorie_stats:
    if i >60:
        more_than_60.append(i)
more_calories = round(len(more_than_60)/len(calorie_stats)*100,2)
print(more_calories)

#Это действительно высокий процент. Это будет очень полезно, когда мы будем продвигать CrunchieMunchies. Но один вопрос
# заключается в том, насколько велики различия в наборе данных? Можем ли мы сделать обобщение, что в большинстве злаков
# содержится около 100 калорий или разброс еще больше? Рассчитайте величину отклонения, найдя стандартное отклонение,
# Сохраните свой ответ в calorie_std и распечатайте на терминале. Как мы можем включить эту ценность в наш анализ?

calorie_std = np.std(calorie_stats)
print(calorie_std)
# Отклонение показывает как арспределены значения относительно среднего, что может быть полезно для понимания насколько
# значения выборки могут отличаться от среднего

#Напишите короткий абзац, в котором кратко изложите свои выводы и то, как, по вашему мнению, эти данные могут быть
# использованы в интересах Mycrunch при маркетинге CrunchieMunchies.
# Прежде всего данные помогут построить стратегию маркетинга компании, а также правильно использовать имеющиеся статистические
# данные и показать продукт с выгодной стороны. Кроме того, данные могут быть использованы для пониманию дальнейшего
# пути развития продукта.
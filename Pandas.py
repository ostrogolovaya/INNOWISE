import pandas as pd
import numpy as np
data = pd.read_csv("./adult.data.csv")

#1. Посчитайте, сколько мужчин и женщин (признак sex) представлено в этом датасете
y = sum(data['sex'] =='Male')
x = sum(data['sex'] =='Female')
print('Female - ' + str(x) + ', male - ' +str(y))

#2. Каков средний возраст мужчин (признак age) по всему датасету?
avg_age = data[data['sex'] == 'Male']['age'].mean()
print(round(avg_age,2))

#3. Какова доля граждан Соединенных Штатов (признак native-country)?
usa_cit = (data['native-country'] == 'United-States').mean()
print(round(usa_cit*100,2))

#4-5. Рассчитайте среднее значение и среднеквадратичное отклонение возраста тех, кто получает более 50K в год (признак
# salary) и тех, кто получает менее 50K в год
avg_more_50 = data[data['salary'] == '>50K']['age'].mean()
avg_less_50 = data[data['salary'] == '<=50K']['age'].mean()
std_more_50 = data[data['salary'] == '>50K']['age'].std()
std_less_50 = data[data['salary'] == '<=50K']['age'].std()
print(round(avg_less_50,2),round(avg_more_50,2),round(std_less_50,2),round(std_more_50,2))

#6. Правда ли, что люди, которые получают больше 50k, имеют минимум высшее образование? (признак education – Bachelors,
# Prof-school, Assoc-acdm, Assoc-voc, Masters или Doctorate)

get_50k_w_ed = data[(data['salary'] == '>50K') & (data['education'].isin(['Bachelors','Prof-school', 'Assoc-acdm', 'Assoc-voc','Masters','Doctorate']))].shape[0]
get_50k = (data['salary'] == '>50K').shape[0]
result = get_50k_w_ed/get_50k*100
#print(result)
#Нет, не правда

#7. Выведите статистику возраста для каждой расы (признак race) и каждого пола. Используйте groupby и describe. Найдите
# таким образом максимальный возраст мужчин расы Asian-Pac-Islander.

stat = data[['race','age','sex']].groupby(['race', 'sex']).describe()
print(stat)
# максимальный возраст мужчин расы Asian-Pac-Islander - 90

#8. Среди кого больше доля зарабатывающих много (>50K): среди женатых или холостых мужчин (признак marital-status)?
# Женатыми считаем тех, у кого marital-status начинается с Married (Married-civ-spouse, Married-spouse-absent или
# Married-AF-spouse), остальных считаем холостыми.

get_50k = (data['salary'] == '>50K').shape[0]
married = round((data[(data['salary'] == '>50K') & (data['marital-status'].str.startswith('Married'))].shape[0])/get_50k * 100,2)
not_married = round(100 - married,2)
print(married, not_married)
# среди холостых больше доля зарабатывающих много (>50K)

#9. Какое максимальное число часов человек работает в неделю (признак hours-per-week)? Сколько людей работают такое
# количество часов и каков среди них процент зарабатывающих много?

max_hours = data['hours-per-week'].max()
people_work_max = (data['hours-per-week'] == data['hours-per-week'].max()).value_counts()
perc_50k = ((data['hours-per-week'] == data['hours-per-week'].max()) & (data['salary'] == '>50K')).mean()
print(max_hours, people_work_max)
print(round(perc_50k*100,2))

#10. Посчитайте среднее время работы (hours-per-week) зарабатывающих мало и много (salary) для каждой страны
# (native-country).

avg_wrk_time = data[['native-country','hours-per-week','salary']].groupby(['native-country','salary'])['hours-per-week'].mean()
print(avg_wrk_time)

#11.Сгруппируйте людей по возрастным группам young, adult, retiree, где:
# young соответствует 16-35 лет
# adult - 35-70 лет
# retiree - 70-100 лет
# Проставьте название соответсвтуещей группы для каждого человека в новой колонке AgeGroup

data['AgeGroup'] = np.where( data['age'].between(16,35,inclusive = 'left'), 'Young',
                   np.where( data['age'].between(35,70,inclusive = 'left'), 'Adult',
                   np.where( data['age'].between(70,100,inclusive = 'left'), 'Retiree', 'None')))
print(data['AgeGroup'])

#12-13. Определите количество зарабатывающих >50K в каждой из возрастных групп (колонка AgeGroup), а также выведите
# название возрастной группы, в которой чаще зарабатывают больше 50К (>50K)

earn_more_than_50 = data[data['salary'] == '>50K'].groupby('AgeGroup')['AgeGroup'].value_counts()
print(earn_more_than_50)

young = data[(data['salary'] == '>50K') & (data['AgeGroup'] == 'Young')].shape[0]
adult = data[(data['salary'] == '>50K') & (data['AgeGroup'] == 'Adult')].shape[0]
retiree = data[(data['salary'] == '>50K') & (data['AgeGroup'] == 'Retiree')].shape[0]

if (young>adult and young>retiree):
 print('young')
elif (adult>young and adult>retiree):
 print('adult')
elif (retiree>young and retiree>adult):
 print('retiree')
elif (adult==young and young>retiree):
 print('adult and young')
elif (retiree==young and retiree>adult):
 print('retiree and young')
elif (retiree==adult and retiree>young):
 print('retiree and adult')

#14. Сгруппируйте людей по типу занятости (колонка occupation) и определите количество людей в каждой группе. После чего
# напишите функциюю фильтрации filter_func, которая будет возвращать только те группы, в которых средний возраст
# (колонка age) не больше 40 и в которых все работники отрабатывают более 5 часов в неделю (колонка hours-per-week)

occ = data.groupby('occupation')['occupation'].count()
print(occ)
def filter_func():
 filtered_data = data.groupby('occupation').filter(lambda x: (x['hours-per-week'].min() > 5) & (x['age'].mean() <= 40))
 return ((filtered_data.groupby('occupation')['hours-per-week'].min()).keys())
print(filter_func())
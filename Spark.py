import pyspark
import psycopg2
import py4j
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import col, count, sum
from pyspark.sql.functions import *
from pyspark import SparkConf, SparkContext

spark = SparkSession.builder.appName("PostgreSQL_JDBC_Driver")\
    .config("spark.jars", "/C:/Users/ostro/INNOWISE/PySpark/postgresql-42.5.0.jar").getOrCreate()

actor = spark.read.format("jdbc").option("url", "jdbc:postgresql://localhost:5432/postgres") \
    .option("driver", "org.postgresql.Driver").option("dbtable", "public.actor") \
    .option("user", "postgres").option("password", "180896").load()
#print(actor.show())

address = spark.read.format("jdbc").option("url", "jdbc:postgresql://localhost:5432/postgres") \
    .option("driver", "org.postgresql.Driver").option("dbtable", "public.address") \
    .option("user", "postgres").option("password", "180896").load()
#print(address.show())

category = spark.read.format("jdbc").option("url", "jdbc:postgresql://localhost:5432/postgres") \
    .option("driver", "org.postgresql.Driver").option("dbtable", "public.category") \
    .option("user", "postgres").option("password", "180896").load()
#print(category.show())

city = spark.read.format("jdbc").option("url", "jdbc:postgresql://localhost:5432/postgres") \
    .option("driver", "org.postgresql.Driver").option("dbtable", "public.city") \
    .option("user", "postgres").option("password", "180896").load()
#print(city.show())

country = spark.read.format("jdbc").option("url", "jdbc:postgresql://localhost:5432/postgres") \
    .option("driver", "org.postgresql.Driver").option("dbtable", "public.country") \
    .option("user", "postgres").option("password", "180896").load()
#print(country.show())

customer = spark.read.format("jdbc").option("url", "jdbc:postgresql://localhost:5432/postgres") \
    .option("driver", "org.postgresql.Driver").option("dbtable", "public.customer") \
    .option("user", "postgres").option("password", "180896").load()
#print(customer.show())

film = spark.read.format("jdbc").option("url", "jdbc:postgresql://localhost:5432/postgres") \
    .option("driver", "org.postgresql.Driver").option("dbtable", "public.film") \
    .option("user", "postgres").option("password", "180896").load()
#print(film.show())

film_actor = spark.read.format("jdbc").option("url", "jdbc:postgresql://localhost:5432/postgres") \
    .option("driver", "org.postgresql.Driver").option("dbtable", "public.film_actor") \
    .option("user", "postgres").option("password", "180896").load()
#print(film_actor.show())

film_category = spark.read.format("jdbc").option("url", "jdbc:postgresql://localhost:5432/postgres") \
    .option("driver", "org.postgresql.Driver").option("dbtable", "public.film_category") \
    .option("user", "postgres").option("password", "180896").load()
#print(film_category.show())

inventory = spark.read.format("jdbc").option("url", "jdbc:postgresql://localhost:5432/postgres") \
    .option("driver", "org.postgresql.Driver").option("dbtable", "public.inventory") \
    .option("user", "postgres").option("password", "180896").load()
#print(country.inventory())

language = spark.read.format("jdbc").option("url", "jdbc:postgresql://localhost:5432/postgres") \
    .option("driver", "org.postgresql.Driver").option("dbtable", "public.language") \
    .option("user", "postgres").option("password", "180896").load()
#print(language.show())

payment = spark.read.format("jdbc").option("url", "jdbc:postgresql://localhost:5432/postgres") \
    .option("driver", "org.postgresql.Driver").option("dbtable", "public.payment") \
    .option("user", "postgres").option("password", "180896").load()
#print(payment.show())

rental = spark.read.format("jdbc").option("url", "jdbc:postgresql://localhost:5432/postgres") \
    .option("driver", "org.postgresql.Driver").option("dbtable", "public.rental") \
    .option("user", "postgres").option("password", "180896").load()
#print(rental.show())

staff = spark.read.format("jdbc").option("url", "jdbc:postgresql://localhost:5432/postgres") \
    .option("driver", "org.postgresql.Driver").option("dbtable", "public.staff") \
    .option("user", "postgres").option("password", "180896").load()
#print(staff.show())

store = spark.read.format("jdbc").option("url", "jdbc:postgresql://localhost:5432/postgres") \
    .option("driver", "org.postgresql.Driver").option("dbtable", "public.store") \
    .option("user", "postgres").option("password", "180896").load()
#print(store.show())

#1. Вывести количество фильмов в каждой категории, отсортировать по убыванию.

a = film_category.join(category, film_category.category_id == category.category_id,"inner")\
    .select(film_category.film_id,film_category.category_id,category.name)
a.groupBy("name").agg(count(a.film_id).alias("amount_of_films")).sort(col("amount_of_films").desc()).show()

#2. Вывести 10 актеров, чьи фильмы большего всего арендовали, отсортировать по убыванию.

b = actor.join(film_actor, actor.actor_id == film_actor.actor_id, "inner")\
    .join(film, film_actor.film_id == film.film_id, "inner")\
    .join(inventory, film.film_id == inventory.film_id, "inner")\
    .join(rental, inventory.inventory_id == rental.inventory_id, "inner")\
    .select(actor.first_name, actor.last_name, rental.rental_id)
b.groupBy("last_name", "first_name").agg(count(b.rental_id).alias("amount_of_films")).sort(col("amount_of_films").desc()).show(10)

#3. Вывести категорию фильмов, на которую потратили больше всего денег.

d = category.join(film_category, category.category_id == film_category.category_id, "inner")\
    .join(film, film_category.film_id == film.film_id, "inner")\
    .join(inventory, film.film_id == inventory.film_id, "inner")\
    .join(rental, inventory.inventory_id == rental.inventory_id, "inner")\
    .join(payment, payment.rental_id == rental.rental_id, "inner")\
    .groupBy(category.name).agg(sum(payment.amount).alias("sum_of_revenue")).sort(col("sum_of_revenue").desc()).select(category.name).show(1)

#4. Вывести названия фильмов, которых нет в inventory

inv = inventory.select(inventory.film_id,inventory.inventory_id).distinct().collect()
inv_list =[]
for i,j in inv:
    inv_list.append(i)

film.filter(~film.film_id.isin(inv_list)).select(film.title).show(60)

#5. Вывести топ 3 актеров, которые больше всего появлялись в фильмах в категории “Children”. Если у нескольких актеров
#одинаковое кол-во фильмов, вывести всех.
amt_of_films = category.join(film_category, category.category_id == film_category.category_id, "inner")\
    .join(film, film_category.film_id == film.film_id, "inner")\
    .join(film_actor, film.film_id == film_actor.film_id, "inner")\
    .join(actor, film_actor.actor_id == actor.actor_id, "inner")\
    .filter(category.name == 'Children').groupBy(actor.last_name, actor.first_name)\
    .agg(count(film.film_id).alias("amt_of_films")).sort(col("amt_of_films").desc()).limit(3)\
    .select("amt_of_films",actor.last_name, actor.first_name).collect()

new_list=[]
for i,j,h in amt_of_films:
    new_list.append(i)

pop_actors = category.join(film_category, category.category_id == film_category.category_id, "inner")\
    .join(film, film_category.film_id == film.film_id, "inner")\
    .join(film_actor, film.film_id == film_actor.film_id, "inner")\
    .join(actor, film_actor.actor_id == actor.actor_id, "inner")\
    .filter(category.name == 'Children').groupBy(actor.last_name, actor.first_name)\
    .agg(count(film.film_id).alias("number_of_films")).filter(col("number_of_films").isin(new_list))\
    .select(actor.first_name, actor.last_name).show()



#6. Вывести города с количеством активных и неактивных клиентов (активный — customer.active = 1). Отсортировать по
# количеству неактивных клиентов по убыванию.
city_1 = city.alias("city_1")
city_2 = city.alias("city_2")
active_clients = city_1.join(address, city_1.city_id == address.city_id, "inner")\
    .join(customer, customer.address_id == address.address_id, "inner")\
    .filter(customer.active == 1).groupBy(city_1.city_id, city_1.city)\
    .agg(count(customer.customer_id).alias("active_clients"))\
    .select(city_1.city_id.alias("city_id1"), city_1.city.alias("city1"), "active_clients")

inactive_clients = city_2.join(address, city_2.city_id == address.city_id, "inner")\
    .join(customer, customer.address_id == address.address_id, "inner")\
    .filter(customer.active == 0).groupBy(city_2.city_id, city_2.city)\
    .agg(count(customer.customer_id).alias("inactive_clients"))\
    .select(city_2.city_id.alias("city_id2"), city_2.city.alias("city2"), "inactive_clients")

result = city.join(active_clients, city.city_id == active_clients.city_id1, "left")\
    .join(inactive_clients, city.city_id == inactive_clients.city_id2, "left")\
    .select(city.city_id, city.city, inactive_clients.inactive_clients, active_clients.active_clients)\
    .sort(inactive_clients.inactive_clients.desc()).show()\

#7. Вывести категорию фильмов, у которой самое большое кол-во часов суммарной аренды в городах (customer.address_id в
# этом city), и которые начинаются на букву “a”. Тоже самое сделать для городов в которых есть символ “-”.
#
diff_secs_col = rental.withColumn('from_timestamp',to_timestamp(col('return_date')))\
    .withColumn('to_timestamp',to_timestamp(col('rental_date')))\
    .withColumn('dif_in_col',col('from_timestamp').cast("long") - col('to_timestamp').cast("long"))
rental_add = diff_secs_col.withColumn("diff_hours", col('dif_in_col')/3600)

pop_cat_a = city.join(address, city.city_id==address.city_id,"inner")\
    .join(customer, address.address_id==customer.address_id,"inner")\
    .join(rental_add, rental_add.customer_id==customer.customer_id,"inner")\
    .join(inventory, rental_add.inventory_id==rental.inventory_id,"inner")\
    .join(film, film.film_id==inventory.film_id,"inner")\
    .join(film_category, film.film_id==film_category.film_id, "inner")\
    .join(category,category.category_id==film_category.category_id,"inner")\
    .where(col('city').like('A%'))\
    .groupby(category.name).agg(sum(rental_add.diff_hours).alias('df_hours')).sort(col('df_hours').desc()).select(category.name).show(1)

pop_cat = city.join(address, city.city_id==address.city_id,"inner")\
    .join(customer, address.address_id==customer.address_id,"inner")\
    .join(rental_add, rental_add.customer_id==customer.customer_id,"inner")\
    .join(inventory, rental_add.inventory_id==rental.inventory_id,"inner")\
    .join(film, film.film_id==inventory.film_id,"inner")\
    .join(film_category, film.film_id==film_category.film_id, "inner")\
    .join(category,category.category_id==film_category.category_id,"inner")\
    .where(col('city').like('%-%'))\
    .groupby(category.name).agg(sum(rental_add.diff_hours).alias('df_hours')).sort(col('df_hours').desc()).select(category.name).show(1)

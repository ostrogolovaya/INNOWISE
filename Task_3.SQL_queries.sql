/*1. Вывести количество фильмов в каждой категории, отсортировать по убыванию.*/
select c.name, count(*)
from category c 
inner join film_category fc on c.category_id = fc.category_id 
inner join film f on fc.film_id = f.film_id 
group by c.name 
order by count(*) desc

/*2. Вывести 10 актеров, чьи фильмы большего всего арендовали, отсортировать по убыванию.*/
select first_name, last_name 
from (select a.first_name, a.last_name
, count(r.rental_id) as amount
from actor a 
inner join film_actor fa on a.actor_id = fa.actor_id 
inner join film f on f.film_id=fa.film_id 
inner join inventory i on f.film_id=i.film_id 
inner join rental r on i.inventory_id=r.inventory_id 
group by last_name, first_name 
order by amount desc) as top_actors
limit(10)

/*3. Вывести категорию фильмов, на которую потратили больше всего денег.*/
select   name from (
select c.name, sum(amount) as sum_film
from category c 
inner join film_category fc on c.category_id=fc.category_id 
inner join film f on fc.film_id=fc.film_id 
inner join inventory i on i.film_id=f.film_id 
inner join rental r on r.inventory_id=i.inventory_id 
inner join payment p on p.rental_id=r.rental_id 
group by c."name" 
order by sum_film desc) as a
limit(1)

/*4. Вывести названия фильмов, которых нет в inventory. Написать запрос без использования оператора IN.*/
select title 
from ((select title, film_id
from film f
order by film_id)
except
(select distinct title, i.film_id
from film f 
inner join inventory i on f.film_id = i.film_id
order by film_id)) as a

/*5. Вывести топ 3 актеров, которые больше всего появлялись в фильмах в категории “Children”.
Если у нескольких актеров одинаковое кол-во фильмов, вывести всех.*/
select a.first_name, a.last_name, count(title) as amount_of_films
from category c 
inner join film_category fc on c.category_id=fc.category_id 
inner join film f on fc.film_id=fc.film_id 
inner join film_actor fa on f.film_id = fa.film_id 
inner join actor a on fa.actor_id = a.actor_id 
where c.name = 'Children'
group by a.last_name,a.first_name
having count(title) in (select count(title) from category c 
inner join film_category fc on c.category_id=fc.category_id 
inner join film f on fc.film_id=fc.film_id 
inner join film_actor fa on f.film_id = fa.film_id 
inner join actor a on fa.actor_id = a.actor_id 
where c.name = 'Children'
group by a.last_name,a.first_name
order by count(title) desc 
limit 3)

/*6. Вывести города с количеством активных и неактивных клиентов (активный — customer.active = 1).
Отсортировать по количеству неактивных клиентов по убыванию.*/
select c.city, inactive_cl.amount as inactive_clients, active_cl.amount as active_clients
from city c 
left join (select c.city_id,city, count(*) as amount
from city c 
inner join address a on c.city_id=a.city_id
inner join customer cu on cu.address_id=a.address_id
where cu.active = 1
group by c.city_id,c.city) as active_cl 
on c.city_id=active_cl.city_id
left join (select c.city_id, c.city, count(*) as amount
from city c 
inner join address a on c.city_id=a.city_id
inner join customer cu on cu.address_id=a.address_id
where cu.active = 0
group by c.city_id, c.city) as inactive_cl
on c.city_id=inactive_cl.city_id
group by c.city_id, c.city, inactive_clients, active_clients
order by inactive_cl.amount desc nulls last

/*7. Вывести категорию фильмов, у которой самое большое кол-во часов суммарной аренды в городах (customer.address_id в этом city),
 и которые начинаются на букву “a”. То же самое сделать для городов в которых есть символ “-”. Написать все в одном запросе.*/

(select  'City started with A' as city , name
from(select  sum((date(r.return_date)-date(r.rental_date))*24 + extract(hour from (return_date - rental_date))) as rental_sum
, ca.name
from rental r
inner join inventory i on r.inventory_id=i.inventory_id 
inner join film f on f.film_id=i.film_id 
inner join film_category fc on f.film_id=fc.film_id 
inner join category ca on ca.category_id=fc.category_id 
inner join customer cu on cu.customer_id=r.customer_id 
inner join address a on cu.address_id=a.address_id 
inner join city c on a.city_id=c.city_id 
where lower(city) like lower('a%')
group by ca.name
order by rental_sum desc 
limit(1)) as a)
union all
(select 'City with -' as city, name  
from(select  sum((date(r.return_date)-date(r.rental_date))*24 + extract(hour from (return_date - rental_date))) as rental_sum
, ca.name
from rental r
inner join inventory i on r.inventory_id=i.inventory_id 
inner join film f on f.film_id=i.film_id 
inner join film_category fc on f.film_id=fc.film_id 
inner join category ca on ca.category_id=fc.category_id 
inner join customer cu on cu.customer_id=r.customer_id 
inner join address a on cu.address_id=a.address_id 
inner join city c on a.city_id=c.city_id 
where lower(city) like lower('%-%')
group by ca.name
order by rental_sum desc 
limit(1)) as b )

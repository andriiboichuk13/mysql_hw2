use blog_db5;

select * from users
limit 50;

select * from comments
limit 50;

select * from posts
limit 50;

explain analyze
SELECT 
    u.user_id,
    u.username,
    u.email,
    u.country,
    (SELECT COUNT(*) 
     FROM posts p 
     WHERE p.user_id = u.user_id 
       AND YEAR(p.created_at) = 2024
       AND p.category IN ('Technology', 'Travel')) as posts_count,
    (SELECT SUM(p2.views) 
     FROM posts p2 
     WHERE p2.user_id = u.user_id 
       AND YEAR(p2.created_at) = 2024
       AND p2.category IN ('Technology', 'Travel')) as total_views,
    (SELECT COUNT(*) 
     FROM comments c 
     WHERE c.user_id = u.user_id 
       AND YEAR(c.created_at) = 2024) as comments_count,
    (SELECT AVG(c2.likes)
     FROM comments c2
     WHERE c2.user_id = u.user_id
       AND YEAR(c2.created_at) = 2024) as avg_comment_likes
FROM users u
WHERE u.user_id IN (
    SELECT p3.user_id
    FROM posts p3
    WHERE YEAR(p3.created_at) = 2024
      AND p3.category IN ('Technology', 'Travel')
)
AND u.status = 'active'
ORDER BY total_views DESC
LIMIT 100;





EXPLAIN analyze
SELECT u.username, p.title, COUNT(c.comment_id) as comment_count
FROM users u
JOIN posts p ON u.user_id = p.user_id
LEFT JOIN comments c ON p.post_id = c.post_id
WHERE p.category = 'Technology'
GROUP BY u.user_id, p.post_id
ORDER BY comment_count DESC
LIMIT 10;

/*'-> Limit: 10 row(s)  (actual time=19878..19878 rows=10 loops=1)\n    -> Sort: comment_count DESC, limit input to 10 row(s) per chunk  (actual time=19878..19878 rows=10 loops=1)\n        -> Table scan on <temporary>  (actual time=19692..19825 rows=200442 loops=1)\n            -> Aggregate using temporary table  (actual time=19692..19692 rows=200442 loops=1)\n                -> Left hash join (c.post_id = p.post_id)  (cost=23.2e+9 rows=232e+9) (actual time=3327..16484 rows=372730 loops=1)\n                    -> Nested loop inner join  (cost=256479 rows=119515) (actual time=0.798..10578 rows=200442 loops=1)\n                        -> Filter: ((p.category = \'Technology\') and (p.user_id is not null))  (cost=125047 rows=119515) (actual time=0.073..1950 rows=200442 loops=1)\n                            -> Table scan on p  (cost=125047 rows=1.2e+6) (actual time=0.062..1518 rows=1.2e+6 loops=1)\n                        -> Single-row index lookup on u using PRIMARY (user_id=p.user_id)  (cost=1 rows=1) (actual time=0.0426..0.0426 rows=1 loops=200442)\n                    -> Hash\n                        -> Table scan on c  (cost=214 rows=1.94e+6) (actual time=14.9..2505 rows=2e+6 loops=1)\n'
*/

drop index users_indx on users;
drop index posts_indx on posts;
drop index comments_indx on comments;


create index users_indx on users(user_id, username, created_at, status);
create index posts_indx on posts(user_id, post_id, title, category, likes, created_at);
create index comments_indx on comments(comment_id, post_id, likes, created_at);

/*'-> Limit: 10 row(s)  (actual time=12165..12165 rows=10 loops=1)\n    -> Sort: comment_count DESC, limit input to 10 row(s) per chunk  (actual time=12165..12165 rows=10 loops=1)\n        -> Table scan on <temporary>  (actual time=11976..12112 rows=200442 loops=1)\n            -> Aggregate using temporary table  (actual time=11976..11976 rows=200442 loops=1)\n                -> Left hash join (c.post_id = p.post_id)  (cost=23.2e+9 rows=232e+9) (actual time=2397..8716 rows=372730 loops=1)\n                    -> Nested loop inner join  (cost=257532 rows=119515) (actual time=3.8..3708 rows=200442 loops=1)\n                        -> Filter: ((p.category = \'Technology\') and (p.user_id is not null))  (cost=126083 rows=119515) (actual time=0.0924..2075 rows=200442 loops=1)\n                            -> Covering index scan on p using posts_indx  (cost=126083 rows=1.2e+6) (actual time=0.0854..1671 rows=1.2e+6 loops=1)\n                        -> Single-row index lookup on u using PRIMARY (user_id=p.user_id)  (cost=1 rows=1) (actual time=0.00766..0.00775 rows=1 loops=200442)\n                    -> Hash\n                        -> Covering index scan on c using comments_inx  (cost=269 rows=1.94e+6) (actual time=2.21..1141 rows=2e+6 loops=1)\n'
*/

explain analyze
with posts_year_select as (
    select p.user_id, count(*) as count_posts, sum(p.views) as sum_views
    from posts p use index (posts_indx)
    where year(p.created_at) = 2024 AND p.category IN ('Technology', 'Travel')
    group by p.user_id
    ),
comments_year_select as (
	select c.user_id, count(*) as count_comments, avg(c.likes) as avg_likes
    from comments c use index(comments_indx)
    where year(c.created_at) = 2024
    group by c.user_id
)
SELECT 
    u.user_id,
    u.username,
    u.email,
    u.country,
    p.count_posts,
    p.sum_views,
    c.count_comments,
    c.avg_likes
    from users u use index (users_indx)
    INNER JOIN posts_year_select p ON u.user_id = p.user_id
	LEFT JOIN comments_year_select c ON u.user_id = c.user_id
    where u.status = "active"
    ORDER BY p.sum_views DESC
    LIMIT 100;
    
  /*'-> Limit: 100 row(s)  (cost=964727 rows=0) (actual time=51128..51191 rows=100 loops=1)\n    -> Nested loop left join  (cost=964727 rows=0) (actual time=51128..51191 rows=100 loops=1)\n        -> Nested loop inner join  (cost=904970 rows=23903) (actual time=29661..29723 rows=100 loops=1)\n            -> Sort: p.sum_views DESC  (cost=651759..651759 rows=239031) (actual time=29660..29660 rows=285 loops=1)\n                -> Filter: (p.user_id is not null)  (cost=173890..200783 rows=239031) (actual time=29342..29405 rows=182283 loops=1)\n                    -> Table scan on p  (cost=173890..176880 rows=239031) (actual time=29342..29379 rows=182283 loops=1)\n                        -> Materialize CTE posts_year_select  (cost=173890..173890 rows=239031) (actual time=29342..29342 rows=182283 loops=1)\n                            -> Group aggregate: count(0), sum(p.views)  (cost=149986 rows=239031) (actual time=21.6..29195 rows=182283 loops=1)\n                                -> Filter: ((year(p.created_at) = 2024) and (p.category in (\'Technology\',\'Travel\')))  (cost=126083 rows=239031) (actual time=21.6..29044 rows=201243 loops=1)\n                                    -> Index scan on p using posts_indx  (cost=126083 rows=1.2e+6) (actual time=21.6..28612 rows=1.2e+6 loops=1)\n            -> Filter: (u.`status` = \'active\')  (cost=0.959 rows=0.1) (actual time=0.218..0.218 rows=0.351 loops=285)\n                -> Single-row index lookup on u using PRIMARY (user_id=p.user_id)  (cost=0.959 rows=1) (actual time=0.216..0.216 rows=1 loops=285)\n        -> Index lookup on c using <auto_key0> (user_id=p.user_id)  (cost=0.25..2.5 rows=10) (actual time=215..215 rows=0.59 loops=100)\n            -> Materialize CTE comments_year_select  (cost=0..0 rows=0) (actual time=21467..21467 rows=631589 loops=1)\n                -> Table scan on <temporary>  (actual time=16746..17204 rows=631589 loops=1)\n                    -> Aggregate using temporary table  (actual time=16746..16746 rows=631588 loops=1)\n                        -> Filter: (year(c.created_at) = 2024)  (cost=216865 rows=1.94e+6) (actual time=12.4..4760 rows=1e+6 loops=1)\n                            -> Table scan on c  (cost=216865 rows=1.94e+6) (actual time=12.4..4212 rows=2e+6 loops=1)\n'
  */
    
    
    
drop database blog_db;
drop table users;

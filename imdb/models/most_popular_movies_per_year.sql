/*Top rated movies per year*/
select 
  movie_year,
  movie_name,
  movie_rating
from
(
  SELECT 
    m1.movie_year,
    m1.movie_name,
    m1.movie_rating,
    row_number() over (partition by m1.movie_year order by m1.movie_rating desc) as rank
  FROM imdb.top_250_movies as m1
  full join imdb.most_popular_movies as m2 on
    m1.movie_name = m2.movie_name 
  full join imdb.top_english_movies as m3 on
    m1.movie_name = m3.movie_name 
  order by m1.movie_year desc
) as aaa
where 
  aaa.rank = 1


/*Top rated movies per decade*/
select *
from
(
  select 
    movie_year,
    movie_name,
    decade,
    movie_rating,
    row_number() over (partition by decade order by movie_rating desc) as rank
  from
  (
    SELECT 
      m1.movie_year,
      m1.movie_name,
      m1.movie_rating,
      case
        when m1.movie_year between 1920 and 1930 then '20s'
        when m1.movie_year between 1930 and 1940 then '30s'
        when m1.movie_year between 1940 and 1950 then '40s'
        when m1.movie_year between 1950 and 1960 then '50s'
        when m1.movie_year between 1960 and 1970 then '60s'
        when m1.movie_year between 1970 and 1980 then '70s'
        when m1.movie_year between 1980 and 1990 then '80s'
        when m1.movie_year between 1990 and 2000 then '90s'
        when m1.movie_year between 2000 and 2010 then '2000s'
        when m1.movie_year between 2010 and 2020 then '2010s'
        when m1.movie_year between 2020 and 2030 then '2020s'
      end as decade
    FROM imdb.top_250_movies as m1
    full join imdb.most_popular_movies as m2 on
      m1.movie_name = m2.movie_name 
    full join imdb.top_english_movies as m3 on
      m1.movie_name = m3.movie_name 
    order by m1.movie_year desc
  ) as aaa
  where
    aaa.movie_name is not null
) bbb
where 
  bbb.rank = 1
order by 
  bbb.movie_year desc



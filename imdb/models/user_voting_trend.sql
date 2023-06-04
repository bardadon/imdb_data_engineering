/*User voting trend*/

select 
  movie_year,
  total_votes,
  round((total_votes + (LAG(total_votes, 2) over(ORDER BY movie_year)) + (LAG(total_votes, 1) over(ORDER BY movie_year ))) / 3, 2) as Moving_Average_3
from
(
    SELECT 
      m1.movie_year,
      sum(m1.user_votings) as total_votes
    FROM imdb.top_250_movies as m1
    full join imdb.most_popular_movies as m2 on
      m1.movie_name = m2.movie_name 
    full join imdb.top_english_movies as m3 on
      m1.movie_name = m3.movie_name 
    group by
      m1.movie_year
) as aaa
order by movie_year





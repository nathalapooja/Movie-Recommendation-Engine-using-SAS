** Assigning library;
Libname Test '\\filer.uncc.edu\home\vkanugul\Test';
** Loading ratings dataset ;
filename source "\\filer.uncc.edu\home\vkanugul\Test\u.data" ;

data Test.Movies_Ratings ;
  infile
    source dlm='09'x dsd missover ;
  input
    movie_id
    user_id
    rating ;
run ;
proc sort data=TEST.movies_ratings;
by user_id movie_id;
run;

filename source clear ;
** Creating movies_ratings2 dataset which consists of movie_id and user_id with ratings greater than 3;
data Test.movies_ratings2;
set TEST.movies_ratings;
where rating > 3;
run;
** sorting the dataset movies_ratings2;
proc sort data=TEST.movies_ratings2;
by movie_id Descending rating user_id;
run;


** Creating coratings of movies if at least 50 users gives the ratings  ;  
proc sql ;
  create table Test.Pairs as
  select
    a.movie_id ,
    b.movie_id as movie_id_2 ,
    a.rating ,	
    b.rating as rating_2,
    a.user_id 
  from
    Test.Movies_Ratings a
      join Test.Movies_Ratings b on a.user_id = b.user_id
  where
    a.movie_id < b.movie_id
  group by
    a.movie_id, b.movie_id
  having
    count(*) >= 50
  order by
    a.movie_id, b.Movie_id ;
quit ;

** calculating correlation using pairs table and Proc corr ;
proc corr data=Test.Pairs noPrint out=Test.Recommendations (where=(_TYPE_ in ('CORR')) ) pearson;
  by movie_id movie_id_2 ;
  var rating ;
  with rating_2 ;
run ;

** sorting recommendations by movie id ;
proc sort data=Test.Recommendations ;
  by movie_id descending rating ;
run ;
**fetching top 3 movies corelating movies for each movie;
data Test.Recommendations1 ;
  set Test.Recommendations ;
  retain counter ;
  by movie_id ;
  if first.movie_id then counter = 0 ;
  counter + 1 ;
  if counter <= 3 ;
run ;
**Recommending movies to the users based on Movies_ratings2 and Recommendations1 tables;
proc sql ;
  create table Test.Recommendations2 as
  select
    a.user_id ,
    b.movie_id_2 as suggested_movie ,
	b.movie_id as Based_on_Rating_given_to 
  from
    Test.movies_ratings2 a
      join Test.Recommendations1 b on a.movie_id = b.movie_id;

quit ;
**sorting finalrecommendations table;
proc sort data= Test.Recommendations2 nodup;
by user_id suggested_movie Based_on_Rating_given_to ;
run;


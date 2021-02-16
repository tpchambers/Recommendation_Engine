# Theo Chambers - Big Data # 

# Assignment 1 - P3 # 
from mrjob.job import MRJob 
from mrjob.step import MRStep
from sklearn.metrics.pairwise import cosine_similarity
from itertools import combinations 


class Recommender(MRJob):
    def steps(self):
        return [
            MRStep(mapper = self.read_input,reducer=self.reducer),
            MRStep(mapper=self.rating_combinations,reducer=self.compute_similarity),
            MRStep(mapper=self.sorting_mapper,reducer=self.sorting_reducer_output)
        ]
    
    def read_input(self,_,line):
        #parse input and yield customer_id, tuple of movie_name, rating
        (movie_id,customer_id,rating,timestamp,movie_name) = line.split('\t')
        yield customer_id,(movie_name,int(rating))

    def reducer(self,customer_id,ratings):
        #list comprehension outputting tuple of movie_name,rating to each customer_id
        rating_list = [(movie_name,rating) for movie_name,rating in ratings]
        #outputs customer_id, [ratings]
        yield customer_id,rating_list
        
    def rating_combinations(self,customer_id,ratings):
        #iterate over all possible combinations of ratings creating vector size 2 
        for rating1,rating2 in combinations(ratings,2):
            movie_name1 = rating1[0]
            rating_1 = rating1[1]
            movie_name2 = rating2[0]
            rating_2 = rating2[1]
            # we only go in one direction this time, as I do not want repeats of movies going in the opposite direction
            yield (movie_name1,movie_name2), (rating_1,rating_2)
        
    def compute_similarity(self,movie_name,rating_pairs):
        # computing cosine similarity using sklearn package
        #initiate list of movie vectors
        #initiate count
        count = 0 
        movie_1 = []
        movie_2 = []
        #iterating over rating_pairs
        for ratings in rating_pairs:
            #filling in vector of ratings for each movie
            movie_1.append(ratings[0])
            movie_2.append(ratings[1])
            # taking count to keep track of num_pairs
            count += 1
        #taking the cosine_similarity of the vector of movie ratings for each movie
        # I changed the dimensionality of movie_1,movie_2 to [movie_1,movie_2] as sklearn.metrics.cosine_similarity needs a 2d array
        # I subscript the 2d array of [movie_1,movie_2][0][1] as the output is a 2 by 2 matrix and we only want the cosine_similarity score
        cosine_sim = cosine_similarity([movie_1,movie_2])[0][1]
        #filter over number pairs and cosine_similarity of above .99
        if (cosine_sim > .99 and rating_count > 12):
            #swap original location to sort later
            yield cosine_sim, movie_name
    

    ## this last step sorts the output in ascending order
    def sorting_mapper(self,movie_names,pairs):
        yield movie_names, pairs

    def sorting_reducer_output(self,key,values):
        #iterate over values and pull out similarity in ascending order
        for i in values:
            yield i,key
    
if __name__ == '__main__':
    Recommender.run()

# to run from command line # 
# cd (to your directory)
# run python Recommender.py CleanMovieData.txt > P3.txt

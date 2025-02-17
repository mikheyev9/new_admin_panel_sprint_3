# Make task to get the list of indices
get_indices:
	curl -X GET "http://localhost:9200/_cat/indices?v"

# Make task to get the count of movies
count_movies:
	curl -X GET "http://localhost:9200/movies/_count"

# Make task to search movies with match_all query
search_movies:
	curl -X GET "http://localhost:9200/movies/_search?pretty" -H "Content-Type: application/json" -d '
	{
	    "size": 1,
	    "query": {
	        "match_all": {}
	    }
	}'

# Make task to get the count of genres
count_genres:
	curl -X GET "http://localhost:9200/genres/_count"

# Make task to search all genres
search_all_genres:
	curl -X GET "http://localhost:9200/genres/_search?pretty=true" -H "Content-Type: application/json" -d '
	{
	"query": { "match_all": {} },
	"size": 1000
	}'

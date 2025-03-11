.PHONY: setup_directory_for_elasticsearch get_indices count_movies count_persons count_genres search_movies search_all_genres search_all_persons

# Путь к скрипту настройки директорий для Elasticsearch
SETUP_SCRIPT=setup_es_data.sh
setup_directory_for_elasticsearch:
	@echo "🔧 Проверка и настройка директорий перед запуском..."
	@chmod +x $(SETUP_SCRIPT)
	@./$(SETUP_SCRIPT)
	@echo "✅ Директории настроены!"



# Make task to get the list of indices
get_indices:
	curl -X GET "http://localhost:9200/_cat/indices?v"



# Make task to get the count of movies
count_movies:
	curl -X GET "http://localhost:9200/movies/_count"

# Make task to get the count of persons
count_persons:
	curl -X GET "http://localhost:9200/persons/_count"

# Make task to get the count of genres
count_genres:
	curl -X GET "http://localhost:9200/genres/_count"



# Make task to search movies with match_all query
search_movies:
	curl -X GET "http://localhost:9200/movies/_search?pretty" -H "Content-Type: application/json" -d '
	{
	    "size": 1,
	    "query": {
	        "match_all": {}
	    }
	}'

# Make task to search all genres
search_all_genres:
	curl -X GET "http://localhost:9200/genres/_search?pretty=true" -H "Content-Type: application/json" -d '
	{
	"query": { "match_all": {} },
	"size": 1000
	}'


# Make task to search all persons
search_all_persons:
	curl -X GET "http://localhost:9200/persons/_search?pretty=true" -H "Content-Type: application/json" -d '
	{
	"query": { "match_all": {} },
	"size": 1000
	}'
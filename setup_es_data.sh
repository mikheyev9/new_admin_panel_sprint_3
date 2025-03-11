#!/bin/bash
# Директория для данных Elasticsearch
ES_DATA_DIR="./data/es_data"

# Проверка существования директории
if [ ! -d "$ES_DATA_DIR" ]; then
    echo "Создаю директорию: $ES_DATA_DIR"
    sudo mkdir -p "$ES_DATA_DIR"
fi

# Проверка владельца (UID 1000)
OWNER_UID=$(stat -c "%u" "$ES_DATA_DIR")
if [ "$OWNER_UID" -ne 1000 ]; then
    echo "Меняю владельца директории: $ES_DATA_DIR"
    sudo chown -R 1000:1000 "$ES_DATA_DIR"
fi

# Проверка прав доступа
PERMISSIONS=$(stat -c "%a" "$ES_DATA_DIR")
if [ "$PERMISSIONS" != "777" ]; then
    echo "Меняю права доступа на 777: $ES_DATA_DIR"
    sudo chmod -R 777 "$ES_DATA_DIR"
fi

echo "✅ Директории готовы к использованию"
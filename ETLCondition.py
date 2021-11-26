import abc
import json
from typing import Any, Optional
from pathlib import Path


class BaseStorage:
    @abc.abstractmethod
    def save_state(self, state: dict) -> None:
        """Сохранить состояние в постоянное хранилище"""
        pass

    @abc.abstractmethod
    def retrieve_state(self) -> dict:
        """Загрузить состояние локально из постоянного хранилища"""
        pass


class JsonFileStorage(BaseStorage):
    def __init__(self, file_path: Optional[str] = None):
        self.file_path = file_path
        self.file_name = Path(self.file_path)

    def save_state(self, state: dict) -> None:
        if self.file_name.exists():
            with self.file_name.open('r') as state_storage:
                keys = state_storage.read()
        else:
            with self.file_name.open('w') as state_storage:
                keys = json.dumps(state)
                state_storage.write(keys)
        with self.file_name.open('w') as state_storage:
            keys = json.loads(keys)
            keys.update(state)
            keys = json.dumps(keys)
            state_storage.write(keys)

    def retrieve_state(self) -> dict:
        try:
            with self.file_name.open('r') as state_storage:
                keys = json.loads(state_storage.read())
        except FileNotFoundError:
            keys = {}
        return keys


class State:
    """
    Класс для хранения состояния при работе с данными, чтобы постоянно не перечитывать данные с начала.
    Здесь представлена реализация с сохранением состояния в файл.
    В целом ничего не мешает поменять это поведение на работу с БД или распределённым хранилищем.
    """

    def __init__(self, storage: BaseStorage):
        self.storage = storage

    def set_state(self, key: str, value: Any) -> None:
        """Установить состояние для определённого ключа"""
        self.storage.save_state(
            {
                key: value
            }
        )

    def get_state(self, key: str) -> Any:
        """Получить состояние по определённому ключу"""
        current_storage = self.storage.retrieve_state()
        return current_storage.get(key, None)


state_storage = JsonFileStorage()
state_control = State(state_storage)

print(state_control.get_state('key1'), '—', type(state_control.get_state('key1')))
state_control.set_state('key1', 'value1')
print(state_control.get_state('key1'), '—', type(state_control.get_state('key1')))
print(state_control.get_state('key2'), '—', type(state_control.get_state('key2')))
 
state_control.set_state('key1', 'new_value1')
print(state_control.get_state('key1'),  '—', type(state_control.get_state('key1')))
print(state_control.get_state('key2'),  '—', type(state_control.get_state('key2')))
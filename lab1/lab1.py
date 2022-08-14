# lab1
import operator
import json


class Lab1:
                
    def __init__(self) -> None:
        self.json_list = list()
        self.index_file_name = 'index.json'
        self.input_file_name = '2021-04-03-15.json'
        self.output_file_names = ['partition00.json',
                'partition01.json',
                'partition02.json',
                'partition03.json',
                'partition04.json',
                'partition05.json',
                'partition06.json',
                'partition07.json',
                'partition08.json',
                'partition09.json']

        self.read_and_store_file()
        self.write_to_partition_files()

    # None -> None
    def read_and_store_file(self) -> None:
        with open(self.input_file_name, 'r') as file:
            for line in file.readlines():
                self.json_list.append(line)
                file.close()

    # None -> None
    def write_to_partition_files(self) -> None:
        key_range = dict()
        block_size = len(self.json_list) // 10
        line_count = 0
        partition_count = 0

        for line in self.json_list:
            if (line_count == 0):
                output_file = open(self.output_file_names[partition_count], 'w')

            line_count += 1
            output_file.write(line)
            curr_json = json.loads(line)

            #  key_range
            if (line_count == block_size and partition_count != 9):
                key_range[partition_count] = curr_json['id']  # adds key_range
                line_count = 0
                partition_count += 1

        last_json = json.loads(self.json_list[-1])
        key_range['9'] = last_json['id']

        with open('index.json', 'w') as index_file:
            json.dump(key_range, index_file)
            index_file.write('\n')
            index_file.close()

    # event id: int -> str: partition file's name
    def find_partition_by_event_id(self, event_id: int) -> str:
        with open(self.index_file_name, 'r') as file:
            event_range = file.readline()
            json_data = json.loads(event_range)
            file.close()

        for i in range(0, 10):
            if (event_id <= int(json_data[str(i)])):
                return self.output_file_names[i]

    # event id: int -> json string: str
    def first_query(self, event_id: int) -> str:
        part_name = self.find_partition_by_event_id(event_id)

        with open(part_name, 'r') as file:
            part_list = file.readlines()

        for line in part_list:
            json_data = json.loads(line)
            if (int(json_data['id']) == event_id):
                return line

        return 'event id not found.'

    # event id from: int, event id to: int -> generator of json string: gene
    def second_query(self, id_from: int, id_to: int) -> list():
        if (id_from > id_to):
            print('first input must be larger or equal to the second input')
            return

        part_01_name = self.find_partition_by_event_id(id_from)
        part_02_name = self.find_partition_by_event_id(id_to)

        flag = False
        for file_name in self.output_file_names:
            if part_01_name == file_name:
                flag = True

            if (flag == True):
                with open(file_name, 'r') as file:
                    json_data = file.readlines()

                    for line in json_data:
                        curr_data = json.loads(line)
                        curr_event_id = curr_data['id']

                        if (int(curr_event_id) in range(id_from, id_to + 1)):
                            yield line

            if (part_02_name == file_name):
                flag = False

    # None -> dictionary of sorted event type: dict
    def third_query(self) -> dict:
        events_count = dict()

        for file in self.output_file_names:
            with open(file, 'r') as file:
                json_data = file.readlines()

                for line in json_data:
                    curr_data = json.loads(line)
                    event_type = curr_data['type']

                    if (events_count.get(event_type) == None):
                        events_count[event_type] = 0
                    events_count[event_type] = events_count[event_type] + 1

        sorted_d = dict(sorted(events_count.items(), key=operator.itemgetter(1), reverse=True))
        return sorted_d

    # actor's display login name: str -> set of url string: set
    def fourth_query(self, actor: str) -> set:
        url_set = set()

        for file in self.output_file_names:
            with open(file, 'r') as file:
                json_data = file.readlines()

                for line in json_data:
                    curr_data = json.loads(line)

                    if (curr_data['actor']['display_login'] == actor):
                        url_set.add(curr_data['repo']['name'])

        return url_set

    # repository's name: str -> set of actor's display login name
    def fifth_query(self, repo_name: str) -> set:
        actor_set = set()

        for file in self.output_file_names:
            with open(file, 'r') as file:
                json_data = file.readlines()

                for line in json_data:
                    curr_data = json.loads(line)

                    if (curr_data['repo']['name'] == repo_name):
                        actor_set.add(curr_data['actor']['display_login'])

        return actor_set


def main():
    lab1 = Lab1()

    while True:
        choice = input(
                '\n1 - First Query, 2 - Second Query, 3 - Third Query, 4 - Fourth Query, 5 - Fifth Query.\nEnter your choice(1 - 5): ')

        if choice == '1':
            event_id = int(input("enter event id: "))
            print(lab1.first_query(event_id))

        elif choice == '2':
            event_id_from = int(input("enter event id from: "))
            event_id_to = int(input("enter event id to: "))
            for line in lab1.second_query(event_id_from, event_id_to):
                print(line)

        elif choice == '3':
            print(lab1.third_query())

        elif choice == '4':
            actor_display_login = input('enter display actor name: ')
            print(lab1.fourth_query(actor_display_login))

        elif choice == '5':
            repo_name = input('enter repo name: ')
            print(lab1.fifth_query(repo_name))

        else:
            print('invalid input.')


if __name__ == "__main__":
    main()

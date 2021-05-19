# This is a sample Python script.

# Press ⇧F10 to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.

from lithops import Storage

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    storage = Storage()
    obj_metadata = storage.head_object('presupostcatalunya', 'Pressupostos_aprovats_de_la_Generalitat_de_Catalunya.csv')
    print(obj_metadata)


"""

"""
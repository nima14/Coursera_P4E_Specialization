import json
data='''
[
    {
    "name":"Nima",
    "phone":{"Mobile":"09210322520",
            "Home":"01333665050"
            }

    },
    {
    "name":"Amir",
    "phone":{"Mobile":"000",
            "Home":"555"
            }

    }

]'''

info=json.loads(data)


for item in info:
    print('Name:', item["name"])
    print("Phone:",item["phone"]["Home"])
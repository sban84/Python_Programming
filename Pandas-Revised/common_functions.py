def iterate_dictionary(data :dict):
    for (k,v) in data.items():
        print(f"key ={k} and values = {v}")

def sort_dictionary(data:dict):
    for k,v in data.items():
        if isinstance(v, list):
            # sorted(v, reverse=True)
            v.sort(reverse=True)
            print(f"key ={k} and values = {v}")
        else:
            for k, v in sorted(data.items(), key=lambda kv: kv[1], reverse=True):
                print("displaying in sorted order")
                print(f"key = {k} and values={v}")


data = {
    "a":10,
    "b" :1,
    "c" : 100
}

sort_dictionary(data)
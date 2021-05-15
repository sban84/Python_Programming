from collections import Counter

list = [1,2,3,4,1,2,6,7,3,8,1]

cnt = Counter(list)
print(cnt)

# normal_way using loop
result_counter = {}
for i in list:
    if i in result_counter.keys():
        result_counter[i] = result_counter[i] + 1
    else:
        result_counter[i] = 1

print(result_counter)

# most common item

most_common = cnt.most_common()
print(f"Item appeared max times = {most_common[0][0]} and its count = {most_common[0][1]}")

# just for practice how to sort dict by key / value . remember useful
sorted_map ={}
for k,v in sorted(result_counter.items(), key=lambda kv:kv[0]):
    sorted_map[k] = v
print(sorted_map)



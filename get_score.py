import json

gts = json.load(open('data/crud_rag_qa.json'))
# preds = json.load(open('data/crud_rag_kg_results.json'))
preds = json.load(open('data/crud_rag_rag_results.json'))

# preds1 = json.load(open('data/crud_rag_kg_results.json'))
# preds2 = json.load(open('data/crud_rag_rag_results.json'))

# for i in range(len(preds1)):
#     preds1[i]["recalls"].extend(preds2[i]["recalls"])
#     preds1[i]["recalls"] = list(set(preds1[i]["recalls"]))
# preds = preds1

recall_scores = []
for i in range(len(gts)):
    gt = gts[i]["recalls"]
    pred = preds[i]["recalls"]
    tp = len([doc for doc in pred if doc in gt])
    print(tp / len(gt))
    recall_scores.append(tp / len(gt))

print("recall score: ", sum(recall_scores) / len(recall_scores))
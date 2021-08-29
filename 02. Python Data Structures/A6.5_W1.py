text = "X-DSPAM-Confidence:    0.8475";

idx= text.find(':')

res= text[idx+1:]

res =float(res)

print(res)


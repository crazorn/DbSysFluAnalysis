import sql_dataset_creation as gen
import re

gen_wordsh=''
for part in gen.dataset_gen():
    gen_wordsh += part+'\n'

gen_drops=''
for part in gen.drop_views():
    gen_drops += part+'\n'


with open('generated_creates.txt', 'w') as g:
    g.write(gen_wordsh)

with open('generated_drops.txt', 'w') as gd:
    gd.write(gen_drops)



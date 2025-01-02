python -W ignore::FutureWarning ~/edacw1/merizo_search/merizo_search/merizo.py easy-search ~/$1/$2.pdb ~/merizo_search/examples/database/cath-4.3-foldclassdb ./test tmp --iterate --output_headers -d cpu  --threads 3
cat ./test_segment.tsv
cat ./test_search.tsv
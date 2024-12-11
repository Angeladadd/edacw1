from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, IntegerType

def search_partition(partition):
    import uuid
    # from merizo_search.merizo import easy_search, check_for_database


    db_folder = "/home/almalinux/db/cath_foldclassdb/cath-4.3-foldclassdb"
    # check_for_database(db_folder)

    def save_pdb_file(file_path, content):
        with open(file_path, 'w') as f:
            f.write(content)
    
    def process_single_search(filename, content):
        id = filename.split("/")[-1].split(".")[0]
        return id
        # pdb_file = f"/tmp/{id}.pdb"
        # search_output = f"/tmp/{id}"
        # save_pdb_file(pdb_file, content)
        # result = easy_search(pdb_file, db_folder, id, search_output, "cpu", 4)
    
    for filename, content in partition:
        yield process_single_search(filename, content)

    

    

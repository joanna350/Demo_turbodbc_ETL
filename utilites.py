import os
import pandas as pd



def load_by_ext(fn):
    """
    load input in '.xlsx' or '.csv' to pd df
    error control for the rest of the extensions
    """
    ext = fn.split('.')[-1]
    if ext == 'xlsx':
        return pd.read_excel(fn)
    elif ext == 'csv':
        return pd.read_csv(fn)
    else:
        raise TypeError


def getlist(dir):
    """
    to load large set of input files for processing purpose

    Args:
        dir: path from which to list all files and folders existent
    
    Returns:
        flist: List[str] str containst the filenames on the path dir
    """
    flist = os.listdir(dir)
    flist = [i for i in flist if i.split('.')[-1] in ['xlsx', 'csv']]
    return flist

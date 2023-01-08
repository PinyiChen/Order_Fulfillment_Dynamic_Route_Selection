#%%
import os
import pandas as pd
import glob

def save_to_exel(path, file_name):
    all_files = glob.glob(os.path.join(path, "*.csv"))

    writer = pd.ExcelWriter(os.path.join(path, file_name + '.xlsx'), engine='xlsxwriter')

    for f in all_files:
        # print(f)
        df = pd.read_csv(f)
        df.to_excel(writer, sheet_name=os.path.splitext(os.path.basename(f))[0], index=False)

    writer.save()


def make_dir(path):

    if os.path.exists(path) == False:
        os.makedirs(path)
    else:
        print('dir already exist', path)

def skip_init(cls):
    actual_init = cls.__init__
    cls.__init__ = lambda *args, **kwargs: None
    instance = cls()
    cls.__init__ = actual_init
    return instance
    
# %%

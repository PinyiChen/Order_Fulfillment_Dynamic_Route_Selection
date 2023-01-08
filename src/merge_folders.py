import shutil
import os
  
  
# Function to create new folder if not exists
def make_new_folder(folder_name, parent_folder):
      
    # Path
    path = os.path.join(parent_folder, folder_name)
      
    # Create the folder
    # 'new_folder' in
    # parent_folder
    try: 
        # mode of the folder
        mode = 0o777
  
        # Create folder
        os.mkdir(path, mode) 
    except OSError as error: 
        print(error)


def main(current_folder):
    list_dir = ['final_state', 'intermediate_state']
    
    # enumerate on list_dir to get the 
    # content of all the folders ans store 
    # it in a dictionary
    content_list = {}
    for index, val in enumerate(list_dir):
        path = os.path.join(current_folder, val)
        content_list[ list_dir[index] ] = os.listdir(path)
    
    # folder in which all the content will
    # be merged
    merge_folder = "QP"
    
    # merge_folder path - current_folder 
    # + merge_folder
    merge_folder_path = os.path.join(current_folder, merge_folder) 
    
    # create merge_folder if not exists
    make_new_folder(merge_folder, current_folder)

    for sub_dir in content_list:
        # make the path of the content to move 
        path_to_content = sub_dir

        # make the path with the current folder
        dir_to_move = os.path.join(current_folder, path_to_content )

        # move the file
        shutil.move(dir_to_move, merge_folder_path)


import os
import re
from zipfile import ZipFile
from utils import RemoteFileObject
from tables import *
from numpy import array


state_name = 'Rhode Island'
state_abbr = 'ri'

class CensusRow(IsDescription):
    sequence_num = Int64Col()
    logrecno = Int64Col()
    columns = Float32Col(shape=230)
    filetype = StringCol(20)



def remote_fallback_reader(local_base_path, remote_base_url, file_path):
    """ Attempt to read the file locally, but grab from `remote_base_url` if it
    doesn't exist.
    """


    local_path = os.path.join(local_base_path, file_path)

    if os.path.isfile(local_path):
        print 'found file locally'
        return open(local_path, 'r')
    print 'need to fetch file'

    return RemoteFileObject('/'.join([remote_base_url, file_path]))



def load(h5file):
    # create our main sequence group
    group = h5file.createGroup("/",'sequences', 'Sequence files')

    #create table
    table = h5file.createTable(group,"census_row",CensusRow, 'CensusRow')
    row = table.row    

    for file_num in range(118): #118

        acs(file_num+1, 'Tracts_Block_Groups_Only', h5file, row)
        acs(file_num+1, 'All_Geographies_Not_Tracts_Block_Groups', h5file, row)

    table.flush()
    table.cols.logrecno.createIndex()
 


def acs(file_num, geo_type_dir, h5file,row):

    print "Working on File: ", file_num

    base_url = 'http://www2.census.gov/acs2010_5yr/summaryfile/2006-2010_ACSSF_By_State_By_Sequence_Table_Subset/'
    path = os.path.join(
        state_name.replace(' ', ''), geo_type_dir,
        '20105%s%04d000.zip' % (state_abbr, file_num)
    )


    reader = remote_fallback_reader(
        'source',
        base_url,
        path
    )


    z = ZipFile(reader)
    n = z.namelist()
    z.extractall('/tmp/')

    # add the sequence number our row object
    
    


    files = ['e20105%s%04d000.txt' % (state_abbr, file_num),
        'm20105%s%04d000.txt' % (state_abbr, file_num),]


    # work with estimates and moe file.

    file_iter = 0

    for f in files:
  
        file_iter +=1
        z.extract(f, '/tmp/')
        the_file = open('/tmp/' + f, 'r')

        # at this point we have the filename and its in the /tmp/dir
        for line in the_file:

            if file_iter ==1:
                row['filetype'] = "2010e5"
        
            elif file_iter ==2:
                row['filetype'] = '2010m5'

            row['sequence_num'] = file_num
            #print line
            data = str(line).split(',')[5:] # logrecno @ [0]  + only the numbers

                       
            #print "LOGRECNO_"+data[0]
            row['logrecno'] = data[0]
         

            n_arr = array([x for x in filled_list(data[1:], 230)])


            row['columns'] = n_arr

            row.append()

        os.unlink('/tmp/' + f)
        the_file.close()



def val_to_int(value, to = "int"):
    """turns string to int if its empty returns 0 """
    val = 0
    # match only numbers and stuff like 13.5 other wise return false
    p = re.compile('\d+(\.\d+)?')
    m = p.match(value)

    if m is not None:
        if to is "int":
            val = int(m.group())
        else:
            val = float(m.group())
    return val


def filled_list(src_list, targ_len):
    """takes a varible len list and creates a new one with a fixed len()"""
    for i in range(targ_len):
        try:
            yield val_to_int(src_list[i], "float")
        except IndexError:
            yield 0.0





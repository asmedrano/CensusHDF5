import os
from zipfile import ZipFile
from utils import RemoteFileObject
from tables import *
from numpy import array


state_name = 'Rhode Island'
state_abbr = 'ri'

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

    for file_num in range(117):

        print "Reading File: " , file_num
         # create new group for the sequence number
        sequence_group = h5file.createGroup(group, 'Seq_'+ str(file_num+1), 'Sequence '+ str(file_num+1))

        acs(file_num+1, 'Tracts_Block_Groups_Only', h5file)
        acs(file_num+1, 'All_Geographies_Not_Tracts_Block_Groups', h5file)

def acs(file_num, geo_type_dir, h5file):
    base_url = 'http://www2.census.gov/acs2010_5yr/summaryfile/2006-2010_ACSSF_By_State_By_Sequence_Table_Subset/'
    path = os.path.join(
        state_name.replace(' ', ''), geo_type_dir,
        '20105%s%04d000.zip' % (state_abbr, file_num)
    )

    print base_url + path

    reader = remote_fallback_reader(
        '../data/acs2006_2010_5yr/summaryfile/2006-2010_ACSSF_By_State_By_Sequence_Table_Subset',
        base_url,
        path
    )


    z = ZipFile(reader)
    n = z.namelist()
    z.extractall('/tmp/')
    files = ['e20105%s%04d000.txt' % (state_abbr, file_num),
        'm20105%s%04d000.txt' % (state_abbr, file_num),]

    file_iter = 0
    for f in files:
        file_iter +=1
        
        if file_iter == 1:
            #we are looking at the estimates file, create a new group for that
            try:
                group = h5file.getNode("/sequences/"+"Seq_"+str(file_num)+"/estimates")
            except NoSuchNodeError,e:
                group = h5file.createGroup("/sequences/"+"Seq_"+str(file_num), 'estimates', "Estimate file for sequence: " + str(file_num))
        elif file_iter == 2:
            #we are looking at the MOE file, create a new group for that
            try:
                group = h5file.getNode("/sequences/"+"Seq_"+str(file_num)+"/margin_of_error")
            except NoSuchNodeError,e:
                group = h5file.createGroup("/sequences/"+"Seq_"+str(file_num), 'margin_of_error', "MOE file for sequence: " + str(file_num))


        z.extract(f, '/tmp/')

        the_file = open('/tmp/' + f, 'r')
        if not the_file.readline():
            # some files are empty, so just continue to the next
            os.unlink('/tmp/' + f)
            the_file.close()
            continue
        
        # at this point we have the filename and its in the /tmp/dir
        for line in the_file:
            
            data = str(line).split(',')[5:] # only the numbers
            #print "LOGRECNO: ", data[0]
            #turn this values into a numpy array
            n_arr = array([val_to_int(x,'float') for x in data])
            h5file.createArray(group,"LOGRECNO_"+data[0], n_arr)

            #print data

        os.unlink('/tmp/' + f)
        the_file.close()



def val_to_int(value, to = "int"):
    """turns string to int if its empty returns 0 """
    val = 0
    if value is not '' and value != "\n" and value != ".":
        if to is "int":
            val = int(value)
        else:
            val = float(value)
    return val
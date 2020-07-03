"""
Convert GE scanner physiological recording files to BIDS format.
"""

import os
import pydicom
import gzip
import shutil
import json
from datetime import datetime,timedelta
from glob import glob
from collections import OrderedDict


def dicom_load():
    """
    Find all BOLD directories and use the DICOMs to get run end times plus
    task names.
    """
    # Identify folders with EPI data
    dirs = [i for i in os.listdir(dcm_dir) if os.path.isdir(os.path.join(dcm_dir, i))]
    d_cnt = 0
    for d in dirs:
        dcm_file = os.path.join(dcm_dir,d,os.listdir(os.path.join(dcm_dir,d))[0])
        try:
            dcm_data = pydicom.dcmread(dcm_file)
        except:
            pass
        else:
            # If data is EPI then get start time, etc
            if 'EPI' in dcm_data.ImageType:
                dcm_dict[d_cnt] = {}
                dcm_dict[d_cnt]['dcm_file'] = dcm_file
                dcm_dict[d_cnt]['task_name'] = dcm_data.SeriesDescription
                dcm_dict[d_cnt]['task_name'] = dcm_dict[d_cnt]['task_name'].replace('_','-')
                date = dcm_data.SeriesDate
                start = dcm_data.SeriesTime
                start_time = '%s-%s-%s %s:%s:%s'%(date[0:4],date[4:6],date[6:],start[0:2],start[2:4],start[4:])
                dcm_dict[d_cnt]['start_time'] = datetime.fromisoformat(start_time)
                dcm_dict[d_cnt]['run_length'] = dcm_data[0x0019,0x105a].value/1000
                dcm_dict[d_cnt]['end_time'] = dcm_dict[d_cnt]['start_time'] + timedelta(milliseconds=dcm_dict[d_cnt]['run_length'])
                d_cnt = d_cnt+1

def run_numbers():
    """
    When flag is set, identify tasks where there is more than one run and
    assign a run number.

    Otherwise assign output name as task name.
    """
    if run_nos:
        # Get task names
        tasks = []
        for rn in dcm_dict.keys():
            tasks.append(dcm_dict[rn]['task_name'])
        # Assign run numbers
        for tsk in set(tasks):
            n_runs = sum(i == tsk for i in tasks)
            if n_runs == 1:
                for rn in dcm_dict.keys():
                    if dcm_dict[rn]['task_name'] == tsk:
                        # Add in the 'task' prefix required by BIDS format if missing from name
                        if not tsk[0:4] == 'task':
                            dcm_dict[rn]['out_name'] = 'task-'+tsk+'_run-01'
                        else:
                            dcm_dict[rn]['out_name'] = tsk+'_run-01'
            elif n_runs > 1:
                task_runs = []
                run_times = []
                for rn in dcm_dict.keys():
                    if dcm_dict[rn]['task_name'] == tsk:
                        task_runs.append(rn)
                        run_times.append(dcm_dict[rn]['start_time'].timestamp())
                idx_order = sorted(range(len(run_times)), key=lambda k: run_times[k])
                for i in idx_order:
                    if not tsk[0:4] == 'task':
                        dcm_dict[task_runs[i]]['out_name'] = 'task-'+tsk+'_run-0'+str(i+1)
                    else:
                        dcm_dict[task_runs[i]]['out_name'] = tsk+'_run-0'+str(i+1)
    else:
        for rn in dcm_dict.keys():
            dcm_dict[rn]['out_name'] = dcm_dict[rn]['task_name']

def phys_match():
    """
    Identify what physioloigcal files are present for each run. Add these to the
    dictionary.
    """
    # Get list of physiological files
    ppg_files = glob(phys_dir+'PPGData*')
    resp_files = glob(phys_dir+'RESPData*')
    ecg_files = glob(phys_dir+'ECG2Data*')
    # Match to runs
    for rn in dcm_dict.keys():
        # Initiate dictionary entries
        dcm_dict[rn]['ppg_file'] = 'File missing'
        dcm_dict[rn]['resp_file'] = 'File missing'
        dcm_dict[rn]['ecg_file'] = 'File missing'
        # Match time stamp
        # Using only hour and minute due to second mismatch
        # Need to fix
        time_stamp = dcm_dict[rn]['end_time'].strftime('%m%d%Y%H_%M')
        for ppg in ppg_files:
            if time_stamp in ppg:
                dcm_dict[rn]['ppg_file'] = ppg
        for resp in resp_files:
            if time_stamp in resp:
                dcm_dict[rn]['resp_file'] = resp
        for ecg in ecg_files:
            if time_stamp in resp:
                dcm_dict[rn]['ecg_file'] = ecg

def gzip_file(in_file,out_file):
    # Zip file
    with open(in_file, 'rb') as f_in:
        with gzip.open(out_file, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

def make_phys():
    """
    Convert the scanner physiological files to compressed tsv files in the
    relevant output directory.

    Create matching json files.
    """
    for rn in dcm_dict.keys():
        # PPG
        if not dcm_dict[rn]['ppg_file'] == 'File missing':
            # Files
            ppg_tsv = os.path.join(out_dir,subject+'_'+dcm_dict[rn]['out_name']+'_physio-cardiac.tsv.gz')
            ppg_json = os.path.join(out_dir,subject+'_'+dcm_dict[rn]['out_name']+'_physio-cardiac.json')
            # TSV
            gzip_file(dcm_dict[rn]['ppg_file'],ppg_tsv)
            # JSON
            data = OrderedDict()
            data['SamplingFrequency'] = 100.0
            data['StartTime'] = -30.0
            data['Columns'] = 'cardiac'
            with open(ppg_json, 'w') as ff:
                json.dump(data, ff,sort_keys=False, indent=4)
        # Respiration
        if not dcm_dict[rn]['resp_file'] == 'File missing':
            # Files
            resp_tsv = os.path.join(out_dir,subject+'_'+dcm_dict[rn]['out_name']+'_physio-respiratory.tsv.gz')
            resp_json = os.path.join(out_dir,subject+'_'+dcm_dict[rn]['out_name']+'_physio-respiratory.json')
            # TSV
            gzip_file(dcm_dict[rn]['resp_file'],resp_tsv)
            # JSON
            data = OrderedDict()
            data['SamplingFrequency'] = 25.0
            data['StartTime'] = -30.0
            data['Columns'] = 'respiratory'
            with open(resp_json, 'w') as ff:
                json.dump(data, ff,sort_keys=False, indent=4)
        # ECG
        # What to do if they have PPG and ECG?
        if not dcm_dict[rn]['ecg_file'] == 'File missing':
            # Files
            ecg_tsv = os.path.join(out_dir,subject+'_'+dcm_dict[rn]['out_name']+'_physio-cardiac.tsv.gz')
            ecg_json = os.path.join(out_dir,subject+'_'+dcm_dict[rn]['out_name']+'_physio-cardiac.json')
            # TSV
            gzip_file(dcm_dict[rn]['resp_file'],resp_tsv)
            # JSON
            data = OrderedDict()
            data['SamplingFrequency'] = 1000.0
            data['StartTime'] = -30.0
            data['Columns'] = 'cardiac'
            with open(resp_json, 'w') as ff:
                json.dump(data, ff,sort_keys=False, indent=4)

def make_log():
    """
    Create a logfile with details of input and output files.

    File created in folder with original physiological data files.
    """
    log_file = os.path.join(phys_dir,'ge_phys2bids_'+datetime.now().strftime("%Y-%m-%d_%H-%M-%S")+'.log')
    with open(log_file,'w') as log:
        log.write('-------- GE phys2bids --------\n\n')
        log.write('DICOM directory: %s\n'%dcm_dir)
        log.write('Physiology directory: %s\n'%phys_dir)
        log.write('Output directory: %s\n\n'%out_dir)
        log.write('%d EPI files were found\n\n'%len(dcm_dict))
        for rn in dcm_dict.keys():
            log.write('------------------------------\n')
            log.write('%s\n'%dcm_dict[rn]['out_name'])
            log.write('Start time: %s\n'%dcm_dict[rn]['start_time'].strftime("%Y-%m-%d %H:%M:%S"))
            log.write('End time: %s\n'%dcm_dict[rn]['end_time'].strftime("%Y-%m-%d %H:%M:%S"))
            log.write('PPG file: %s\n'%dcm_dict[rn]['ppg_file'])
            log.write('Respiration file: %s\n'%dcm_dict[rn]['resp_file'])
            log.write('ECG file: %s\n'%dcm_dict[rn]['ecg_file'])
            log.write('------------------------------\n\n')

def phys2bids(dcm_dir,phys_dir,out_dir,subject,run_nos=False):
    """
    dcm_dir  -  directory with DICOM file folders
    phys_dir -  directory with the scanner physiological recording files
    out_dir  -  directory to write files to ('func' directory for the subject)
    subject  -  participant ID
    run_nos  -  set to True if you wish the run numbers to be added to file names
                based on run start times (earliest run per task = run-01)
                set to False if run numbers are not to be added (default)
    """
    # Inititate dictionary
    dcm_dict = {}
    # Get details for all EPI runs
    dicom_load()
    # Assign run numbers if required
    run_numbers()
    # Find matching physiological files
    phys_match()
    # Create the BIDS version of the physiological files
    make_phys()
    # Make logfile
    make_log()

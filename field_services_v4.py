import urllib2
import urllib
import json
global headers, url_base
import config as config
import sys, os
import pb_cmd
import wx
import pyodbc
import requests
from datetime import datetime as dt
import time
global gs_dir_path, gs_db_path
from phpserialize import *
from collections import defaultdict
import math

curr_dir = os.getcwd()
gs_dir = curr_dir.split('python_graphing')
gs_dir_path = gs_dir[0]

def usercreate(value):
        url_get = url_base + 'field_services/user/field_services_user/' + config.INPUT_DATA['email'] + '.json'
        get = urllib2.Request(url_get)
        get_response = json.loads(urllib2.urlopen(get).read())
        if value == 1:                        
                if get_response == [False]:
                        url_create = url_base + 'field_services/user/field_services_user'
                        create_data = {}
                        create_data['name'] = config.INPUT_DATA['firstname'] + ' ' + config.INPUT_DATA['lastname']
                        create_data['mail'] = config.INPUT_DATA['email']
                        create_data['pass'] = config.INPUT_DATA['pass']
                        create_data['status'] = config.INPUT_DATA['userstatus']
                        create_encode = urllib.urlencode(create_data)
                        create = urllib2.Request(url_create, create_encode)
  #                      create = urllib2.Request(url_create, create_encode, headers)                          
                        response = urllib2.urlopen(create)
                else:
                        userupdate(0)
        elif value == 0:
                url_create = url_base + 'field_services/user/field_services_user'
                create_data = {}
                create_data['name'] = config.INPUT_DATA['newfirst'] + ' ' + config.INPUT_DATA['newlast']
                create_data['mail'] = config.INPUT_DATA['newemail']
                create_data['status'] = 1
                create_data['pass'] = 'password'
                create_encode = urllib.urlencode(create_data)
                create = urllib2.Request(url_create, create_encode)
#                create = urllib2.Request(url_create, create_encode, headers)
                response = urllib2.urlopen(create)

def userupdate(field):
        url_get = url_base + 'field_services/user/field_services_user/' + config.INPUT_DATA['email'] + '.json'
        get = urllib2.Request(url_get)
        get_response = json.loads(urllib2.urlopen(get).read())
        if get_response == [False] and config.INPUT_DATA['userstatus'] == 1:
                usercreate(0)
        else:
                url_update = url_base + 'field_services/user/field_services_user/' + config.INPUT_DATA['email'] + '.json'
                update_data = {}
                if field == 0:
                        update_data['status'] = config.INPUT_DATA['userstatus']
                        update_data['mail'] = config.INPUT_DATA['email']
                        update_data['name'] = config.INPUT_DATA['firstname'] + ' ' + config.INPUT_DATA['lastname']                                        
                if field == 1:
                        update_data['status'] = config.INPUT_DATA['userstatus']
                elif field == 2:
                        update_data['mail'] = config.INPUT_DATA['newemail']
                elif field == 3:
                        update_data['name'] = config.INPUT_DATA['newfirst'] + ' ' + config.INPUT_DATA['newlast']
                response = requests.put(url_update, update_data)        
                        
def formsindex():
        global formseq
        url_index = url_base + 'field_services/md_form/field_services_md_forms.json'
        index = urllib2.Request(url_index)
        index = json.loads(urllib2.urlopen(index).read())
        if index == [False]:
                print index
        else:
                connection = 'DSN='+config.INPUT_DATA['DSN']+';UID='+config.INPUT_DATA['UID']+';PWD='+config.INPUT_DATA['PWD']
                db_header_table = 'field_services_header'
                header_cnxn = pyodbc.connect(connection)
                header_cursor = header_cnxn.cursor()
                header_cursor.execute('SELECT formseq FROM '+db_header_table)
                row = header_cursor.fetchall()
                header_cnxn.close()
                form_master = {}
                soil_master = {}
                concrete_master = {}
                grout_master = {}
                ole_master = {}
                pickup_master = {}
                form_detail_index = {}
                form_detail = {}
                form_data = {}
                concrete_data = {}
                soil_densities = {}
                soil_specs = {}
                grout_data = {}
                ole_data = {}
                pickup_data = {}
                for j, v in enumerate(index.keys()):
                        form_master[j] = index[v]['master_record']
                        form_detail_index[j] = index[v]['detail_records']
                for i in form_detail_index.keys():
                        for j in form_detail_index[i].keys():
                                form_detail[j] = form_detail_index[i][j]
                for j, v in enumerate(form_detail.keys()):
                        form_data[j] = form_detail[v]
                for i in form_master.keys():
                        form_master[i]['formseq'] = form_master[i]['id']
                        del form_master[i]['id']
                        if form_master[i]['type'] == 'concrete_master':
                                form_master[i]['number_of_sets'] = len(form_detail_index[i])
                                concrete_master[i] = form_master[i]
                        if form_master[i]['type'] == 'pickup_master':
                                form_master[i]['number_of_sets'] = len(form_detail_index[i])
                                pickup_master[i] = form_master[i]
                        if form_master[i]['type'] == 'soil_master':
                                form_master[i]['number_of_sets'] = 1
                                soil_master[i] = form_master[i]
                        if form_master[i]['type'] == 'masonry_cylinders_master' or form_master[i]['type'] == 'masonry_cubes_master':
                                form_master[i]['number_of_sets'] = len(form_detail_index[i])
                                grout_master[i] = form_master[i]
                        if form_master[i]['type'] == 'ole_master' or form_master[i]['type'] == 'ole_pier_master':
                                form_master[i]['number_of_sets'] = len(form_detail_index[i])
                                ole_master[i] = form_master[i]
                cyl_set = []
                cube_set = []
                cube_formseq = []
                cyl_formseq = []
                cube_index = 0
                cyl_index = 0
                cube_value = 0
                cyl_value = 0
                concrete_set_id = defaultdict(list)
                cube_set_id = defaultdict(list)
                cyl_set_id = defaultdict(list)
                pier_page_id = defaultdict(list)
                pier_set_id = defaultdict(list)
                for i in form_data.keys():
                        form_data[i]['formseq'] = form_data[i]['master_id']
                        if form_data[i]['type'] == 'concrete_detail':
                                formseq = form_data[i]['formseq']
                                if gs_db_path == 'prod' or gs_db_path == 'test':
                                        concrete_id = form_data[i]['id']
                                        concrete_set_id[formseq].append(concrete_id)
                                else:
                                        form_data[i]['set_number'] = form_data[i]['field_set_number']['und'][0]['value']
                                if 'field_cure_method' in form_data[i].keys():
                                        if type(form_data[i]['field_cure_method']) is dict:
                                                cure_method = form_data[i]['field_cure_method']['und'][0]['value']
                                                if cure_method == 'Standard w Exceptions':
                                                        form_data[i]['field_cure_method'] = 'StandardNote'
                                                elif cure_method == 'See Remarks':
                                                        form_data[i]['field_cure_method'] = 'Remarks'
                                if 'field_water_unit' in form_data[i].keys():
                                        if type(form_data[i]['field_water_unit']) is dict:
                                                water_unit = form_data[i]['field_water_unit']['und'][0]['value']
                                                if water_unit == 'Lb':
                                                        water_unit = 'L'
                                                elif water_unit == 'Kg':
                                                        water_unit = 'K'
                                                elif water_unit == 'Gal':
                                                        water_unit = 'G'
                                                elif water_unit == 'ltr':
                                                        water_unit = 'T'
                                                form_data[i]['field_water_unit'] = water_unit
                                if 'field_unit_weight' in form_data[i].keys():
                                        if type(form_data[i]['field_unit_weight']) is dict:
                                                if form_data[i]['field_unit_weight']['und'][0]['value'] == '0.0':
                                                        form_data[i]['field_unit_weight'] = []                                        
                                concrete_data[i] = form_data[i]
                        if form_data[i]['type'] == 'pickup_detail':
                                form_data[i]['set_number'] = form_data[i]['field_set_number']['und'][0]['value']
                                if type(form_data[i]['field_cure_method']) is dict:
                                        cure_method = form_data[i]['field_cure_method']['und'][0]['value']
                                        if cure_method == 'Standard w Exceptions':
                                                form_data[i]['field_cure_method'] = 'StandardNote'
                                        elif cure_method == 'See Remarks':
                                                form_data[i]['field_cure_method'] = 'Remarks'                                
                                pickup_data[i] = form_data[i]
                        if form_data[i]['type'] == 'soil_densities_detail':
                                form_data[i]['density_number'] = form_data[i]['field_density_nbr']['und'][0]['value']
                                soil_densities[i] = form_data[i]
                        if form_data[i]['type'] == 'soil_specifications_detail':
                                form_data[i]['spec_number'] = form_data[i]['field_material_no']['und'][0]['value']
                                soil_specs[i] = form_data[i]
                        if form_data[i]['type'] == 'masonry_cubes_detail':
                                formseq = form_data[i]['formseq']
                                cube_id = form_data[i]['id']
                                cube_set_id[formseq].append(cube_id)
                                grout_data[i] = form_data[i]
                        if form_data[i]['type'] == 'masonry_cylinders_detail':
                                formseq = form_data[i]['formseq']
                                cyl_id = form_data[i]['id']
                                cyl_set_id[formseq].append(cyl_id)
                                grout_data[i] = form_data[i]
                        if form_data[i]['type'] == 'ole_steel_detail':
                                form_data[i]['page_number'] = form_data[i]['field_set_number']['und'][0]['value']
                                form_data[i]['field_sheet_name'] = 'Rebar'
                                ole_data[i] = form_data[i]
                        if form_data[i]['type'] == 'ole_pier_detail':
                                form_data[i]['field_sheet_name'] = 'Pier'
                                formseq = form_data[i]['formseq']
                                pier_id = form_data[i]['id']
                                pier_set_id[formseq].append(pier_id)
                                ole_data[i] = form_data[i]                                
                if concrete_set_id != {}:
                        for i in concrete_set_id.keys():
                                set_id = sorted(concrete_set_id[i])
                                for j in range(len(set_id)):
                                        for k in concrete_data.keys():
                                                if concrete_data[k]['id'] == set_id[j]:
                                                        concrete_data[k]['set_number'] = j + 1
                if cube_set_id != {}:
                        for i in cube_set_id.keys():
                                set_id = sorted(cube_set_id[i])
                                for j in range(len(set_id)):
                                        for k in grout_data.keys():
                                                if grout_data[k]['id'] == set_id[j]:
                                                        grout_data[k]['set_number'] = j + 1
                if cyl_set_id != {}:
                        for i in cyl_set_id.keys():
                                set_id = sorted(cyl_set_id[i])
                                for j in range(len(set_id)):
                                        for k in grout_data.keys():
                                                if grout_data[k]['id'] == set_id[j]:                                                        
                                                        grout_data[k]['set_number'] = j + 1
                if pier_set_id != {}:
                        for i in pier_set_id.keys():
                                set_id = sorted(pier_set_id[i])
                                for j in range(len(set_id)):
                                        page = math.trunc(math.ceil((j + 1)/3.0))
                                        for k in ole_data.keys():
                                                if ole_data[k]['id'] == set_id[j]:
                                                        ole_data[k]['page_number'] = page
                                                        ole_data[k]['specimen_nbr'] = (j + 1) - (page - 1) * 3
                if row != []:
                        for item in row:
                                for a in form_master.keys():
                                        if int(form_master[a]['formseq']) in item:
                                                if form_master[a]['type'] == 'concrete_master':
                                                        del concrete_master[a]
                                                if form_master[a]['type'] == 'pickup_master':
                                                        del pickup_master[a]                                                        
                                                if form_master[a]['type'] == 'soil_master':
                                                        del soil_master[a]
                                                if form_master[a]['type'] == 'masonry_cylinders_master':
                                                        del grout_master[a]
                                                if form_master[a]['type'] == 'masonry_cubes_master':
                                                        del grout_master[a]
                                                if form_master[a]['type'] == 'ole_master':
                                                        del ole_master[a]
                                                del form_master[a]
                                for b in concrete_data.keys():
                                        if int(concrete_data[b]['formseq']) in item:
                                                del concrete_data[b]
                                for c in soil_densities.keys():
                                        if int(soil_densities[c]['formseq']) in item:
                                                del soil_densities[c]
                                for d in soil_specs.keys():
                                        if int(soil_specs[d]['formseq']) in item:
                                                del soil_specs[d]
                                for e in grout_data.keys():
                                        if int(grout_data[e]['formseq']) in item:
                                                del grout_data[e]
                                for f in ole_data.keys():
                                        if int(ole_data[f]['formseq']) in item:
                                                del ole_data[f]
                                for g in pickup_data.keys():
                                        if int(pickup_data[g]['formseq']) in item:
                                                del pickup_data[g]
                if form_master != {}:
                        global_form_header(form_master)
                        time_data = form_times(header_table)
                        update_header_table(header_table, time_data)
                        if 'concrete_master' in header_table['form_type']:
                                db_table = 'field_services_concrete'
                                table_key = 'set_number'
                                concrete_data = standardize_field_keys(concrete_data)
                                if gs_db_path == 'test':
                                        for i in concrete_data.keys():
                                                if type(concrete_data[i]['field_contractor']) is dict:
                                                        contractor = concrete_data[i]['field_contractor']['und'][0]['value']
                                                        if type(concrete_data[i]['field_remarks']) is dict:
                                                                remarks = concrete_data[i]['field_remarks']['und'][0]['value']
                                                                remarks = remarks + '; Sub-Contractor: ' + contractor
                                                        else:
                                                                remarks = 'Sub-Contractor: ' + contractor
                                                        concrete_data[i]['field_remarks'] = remarks
                                        concrete_data = set_mtgroup_others(concrete_data)
                                if gs_db_path == 'mtgroup':
                                        concrete_data = set_mtgroup_others(concrete_data)
                                        concrete_data = unserialize_field_info(concrete_data)
                                if gs_db_path == 'prod':
                                        for i in concrete_data.keys():
                                                if type(concrete_data[i]['field_contractor']) is dict:
                                                        contractor = concrete_data[i]['field_contractor']['und'][0]['value']
                                                        if type(concrete_data[i]['field_remarks']) is dict:
                                                                remarks = concrete_data[i]['field_remarks']['und'][0]['value']
                                                                remarks = remarks + '; Sub-Contractor: ' + contractor
                                                        else:
                                                                remarks = 'Sub-Contractor: ' + contractor
                                                        concrete_data[i]['field_remarks'] = remarks                                                                                                                
                                        concrete_data = set_standard_weather(concrete_data)
                                update_data_table(concrete_data, db_table, table_key)
                        if 'soil_master' in header_table['form_type']:
                                for i in soil_master.keys():
                                        if gs_db_path == 'prod':
                                                soil_master[i]['field_soil_test_guage_date'] = soil_master[i]['field_soil_test_date_tested']
                                        if gs_db_path == 'rone':
                                                if type(soil_master[i]['field_gauge_make']) is dict:
                                                        gauge_make = soil_master[i]['field_gauge_make']['und'][0]['value']
                                                else:
                                                        gauge_make = ''
                                                if type(soil_master[i]['field_gauge_model']) is dict:
                                                        gauge_model = soil_master[i]['field_gauge_model']['und'][0]['value']
                                                else:
                                                        gauge_model = ''
                                                if gauge_make != '':
                                                        gauge = gauge_make
                                                if gauge_model != '':
                                                        if gauge != '':
                                                                gauge = gauge + ' ' + gauge_model
                                                        else:
                                                                gauge = gauge_model
                                                if gauge != '':
                                                        soil_master[i]['field_soil_test_other'] = gauge
                                        for j in soil_densities.keys():
                                                if soil_master[i]['formseq'] == soil_densities[j]['formseq']:
                                                        soil_densities[j]['field_test_date'] = soil_master[i]['field_soil_test_date_tested']
                                soil_master = format_soil_master(soil_master)
                                soil_master = set_soil_master_equipment(soil_master)                                
                                soil_header = soil_form_header(soil_master)
                                update_soil_header(soil_header)
                                db_table = 'field_services_soil_densities'
                                table_key = 'density_number'
                                for i in soil_densities.keys():
                                        soil_densities[i]['field_material_no'] = soil_densities[i]['field_spec_number']
                                update_data_table(soil_densities, db_table, table_key)
                                db_table = 'field_services_soil_materials'
                                table_key = 'spec_number'
                                if gs_db_path == 'prod':
                                        for i in soil_specs.keys():
                                                if type(soil_specs[i]['field_min_density']) is dict:
                                                        min_density = int(round(float(soil_specs[i]['field_min_density']['und'][0]['value'])))
                                                        if min_density != 0:
                                                                soil_specs[i]['field_required_density'] = str(min_density) + '% Opt.'
                                update_data_table(soil_specs, db_table, table_key)
                        if 'masonry_cylinders_master' in header_table['form_type'] or 'masonry_cubes_master' in header_table['form_type']:
                                db_table = 'field_services_concrete'
                                table_key = 'set_number'
                                if gs_db_path == 'mtgroup':
                                        for i in grout_data.keys():
                                                if 'field_cyl_spec_size_mt' in grout_data[i].keys():
                                                        grout_data[i]['field_specimen_size'] = grout_data[i]['field_cyl_spec_size_mt']
                                        grout_data = set_mtgroup_others(grout_data)
                                        grout_data = set_material_method(grout_data)
                                        grout_data = unserialize_field_info(grout_data)
                                update_data_table(grout_data, db_table, table_key)
                        if 'ole_master' in header_table['form_type']:
                                table_key = 'page_number'
                                db_table = 'field_services_ole_steel'
                                update_data_table(ole_data, db_table, table_key)
                        if 'ole_pier_master' in header_table['form_type']:
                                table_key = 'page_number'
                                db_table = 'field_services_ole_pier'
                                table_key2 = 'specimen_nbr'
                                ole_data = set_ole_pier_data(ole_data)
                                update_data_table(ole_data, db_table, table_key, table_key2)
                        if 'pickup_master' in header_table['form_type']:
                                db_table = 'field_services_concrete'
                                table_key = 'set_number'
                                update_data_table(pickup_data, db_table, table_key)
                else:
                        index = [False]
                        print index

def global_form_header(form_data):
        global siteseq, taskseq, workorderreqseq, header_table
        formseq = []
        siteseq = []
        workorderreqseq = []
        taskseq = []
        form_type = []
        status = []
        set_number = []
        orig_taskseq = []
        user_seq = []
        field_weather = []
        field_temperature = []
        field_temperature2 = []        
        header_table = {}
        for i in form_data.keys():
                formseq.append(form_data[i]['formseq'])
                siteseq.append(form_data[i]['siteseq'])
                workorderreqseq.append(form_data[i]['workorderreqseq'])
                taskseq.append(form_data[i]['taskseq'])
                form_type.append(form_data[i]['type'])
                status.append(form_data[i]['status'])
                set_number.append(form_data[i]['number_of_sets'])
                orig_taskseq.append(form_data[i]['orig_taskseq'])
                user_seq.append(form_data[i]['user_seq'])
                if gs_db_path == 'mtc' and form_data[i]['type'] == 'soil_master':
                        field_weather.append(form_data[i]['field_weather']['und'][0]['value'])
                        field_temperature.append(form_data[i]['field_temperature']['und'][0]['value'])
                        field_temperature2.append(form_data[i]['field_temperature2']['und'][0]['value'])
                else:
                        field_weather.append([])
                        field_temperature.append([])
                        field_temperature2.append([])
        header_table['formseq'] = formseq
        header_table['siteseq'] = siteseq
        header_table['workorderreqseq'] = workorderreqseq
        header_table['taskseq'] = taskseq
        header_table['form_type'] = form_type
        header_table['status'] = status
        header_table['set_number'] = set_number
        header_table['orig_taskseq'] = orig_taskseq
        header_table['user_seq'] = user_seq
        header_table['field_weather'] = field_weather
        header_table['field_temperature'] = field_temperature
        header_table['field_temperature2'] = field_temperature2

def standardize_field_keys(form_data):
        if gs_db_path == 'paradigm':
                db_path = 'para'
        else:
                db_path = gs_db_path
        for i in form_data.keys():
                for j in form_data[i].keys():
                        field_key = j
                        if db_path in field_key:
                                new_key = field_key.replace('_' + db_path, '')
                                form_data[i][new_key] = form_data[i][field_key]
                        if '_list' in field_key:
                                new_key = field_key.replace('_list', '')
                                form_data[i][new_key] = form_data[i][field_key]
                        if '_mt' in field_key:
                                new_key = field_key.replace('_mt', '')
                                form_data[i][new_key] = form_data[i][field_key]
        return form_data

def format_soil_master(form_data):
        columns = ['field_soil_test_guage_date', 'field_soil_test_remarks', 'field_soil_test_guage_no', 'field_nbr_specimens',
                   'field_material_no', 'field_soil_test_guage_ds', 'field_soil_test_other',
                   'field_soil_test_guage_ms', 'field_soil_test_date_tested', 'field_gauge_make', 'field_gauge_model', 'field_test_mode',
                   'field_description', 'field_material_origin']
        new_columns = ['field_guage_date', 'field_remarks', 'field_guage_no', 'field_nbr_specimens', 'field_material_no',
                       'field_guage_ds', 'field_other', 'field_guage_ms', 'field_date_tested', 'field_gauge_make', 'field_gauge_model', 'field_test_mode',
                       'field_description', 'field_material_origin']
        for i in form_data.keys():
                for k in range(len(columns)):
                        if columns[k] in form_data[i].keys():
                                if type(form_data[i][columns[k]]) is dict:
                                        form_data[i][columns[k]] = form_data[i][columns[k]]['und'][0]['value']
                                form_data[i][new_columns[k]] = form_data[i][columns[k]]
                                if columns[k] not in new_columns[k]:
                                        del form_data[i][columns[k]]
        return form_data

def set_soil_master_equipment(form_data):
        if gs_db_path == 'test' or gs_db_path == 'mtc':
                for i in form_data.keys():
                        density_equipment = []
                        moisture_equipment = []
                        compaction_equipment = []
                        description_string = ''
                        density_list = [1, 2, 3, 4]
                        moisture_list = [1, 2, 3, 4]
                        compaction_list = [1, 2, 3, 4, 5, 6, 7, 8]
                        if type(form_data[i]['field_density_equipment']) is dict:
                                density_checks = form_data[i]['field_density_equipment']['und']
                                for j in range(len(density_checks)):
                                        density_equipment.append(int(density_checks[j]['value']))
                                for k in range(len(density_list)):
                                        if density_list[k] in density_equipment:
                                                char = 'Y'
                                        else:
                                                char = 'N'
                                        description_string = description_string + char
                        if type(form_data[i]['field_moisture_equipment']) is dict:
                                moisture_checks = form_data[i]['field_moisture_equipment']['und']
                                for l in range(len(moisture_checks)):
                                        moisture_equipment.append(int(moisture_checks[l]['value']))
                                for m in range(len(moisture_list)):
                                        if moisture_list[m] in moisture_equipment:
                                                char = 'Y'
                                        else:
                                                char = 'N'
                                        description_string = description_string + char
                        if type(form_data[i]['field_contractor_compaction']) is dict:
                                compaction_checks = form_data[i]['field_contractor_compaction']['und']
                                for n in range(len(compaction_checks)):
                                        compaction_equipment.append(int(compaction_checks[n]['value']))
                                for o in range(len(compaction_list)):
                                        if o != 7:
                                                if compaction_list[o] in compaction_equipment:
                                                        char = 'Y'
                                                else:
                                                        char = 'N'
                                        else:
                                                if type(form_data[i]['field_material_origin']) is dict:
                                                        char = 'Y'
                                                else:
                                                        char = 'N'
                                        description_string = description_string + char
                        form_data[i]['field_description'] = description_string
        else:
                for i in form_data.keys():
                        form_data[i]['field_description'] = []
                        form_data[i]['field_material_origin'] = []
        return form_data

def soil_form_header(form_data):
        soil_header_table = {}
        field_guage_date = []
        field_remarks = []
        field_nbr_specimens = []
        field_material_no = []
        field_guage_ds = []
        field_other = []
        field_guage_ms = []
        field_date_tested = []
        field_guage_no = []
        formseq = []
        field_gauge_make = []
        field_gauge_model = []
        field_test_mode = []
        field_description = []
        field_material_origin = []
        make = ''
        model = ''
        gauge = ''
        for i in form_data.keys():
                field_guage_date.append(form_data[i]['field_guage_date'])
                field_remarks.append(form_data[i]['field_remarks'])
                field_nbr_specimens.append(form_data[i]['field_nbr_specimens'])
                field_material_no.append(form_data[i]['field_material_no'])
                field_guage_ds.append(form_data[i]['field_guage_ds'])
                if gs_db_path == 'paradigm':
                        if form_data[i]['field_gauge_make'] != []:
                                make = form_data[i]['field_gauge_make']
                        else:
                                make = ''
                        if form_data[i]['field_gauge_model'] != []:
                                model = form_data[i]['field_gauge_model']
                        else:
                                model = ''
                        if make != '':
                                gauge = make
                        else:
                                gauge = ''
                        if model != '':
                                if gauge == '':
                                        gauge = model
                                else:
                                        gauge = gauge + ' / ' + model
                        form_data[i]['field_other'] = gauge
                        form_data[i]['field_gauge_make'] = []
                        form_data[i]['field_gauge_model'] = []
                field_gauge_make.append(form_data[i]['field_gauge_make'])
                field_gauge_model.append(form_data[i]['field_gauge_model'])
                field_other.append(form_data[i]['field_other'])
                field_guage_ms.append(form_data[i]['field_guage_ms'])
                field_date_tested.append(form_data[i]['field_date_tested'])
                field_guage_no.append(form_data[i]['field_guage_no'])
                if 'field_test_mode' in form_data[i].keys():
                        field_test_mode.append(form_data[i]['field_test_mode'])
                else:
                        field_test_mode.append([])
                field_description.append(form_data[i]['field_description'])
                field_material_origin.append(form_data[i]['field_material_origin'])
                formseq.append(form_data[i]['formseq'])                
        soil_header_table['formseq'] = formseq
        soil_header_table['field_guage_date'] = field_guage_date
        soil_header_table['field_remarks'] = field_remarks
        soil_header_table['field_nbr_specimens'] = field_nbr_specimens
        soil_header_table['field_material_no'] = field_material_no
        soil_header_table['field_guage_ds'] = field_guage_ds
        soil_header_table['field_other'] = field_other
        soil_header_table['field_gauge_make'] = field_gauge_make
        soil_header_table['field_gauge_model'] = field_gauge_model
        soil_header_table['field_guage_ms'] = field_guage_ms
        soil_header_table['field_date_tested'] = field_date_tested
        soil_header_table['field_guage_no'] = field_guage_no
        soil_header_table['field_test_mode'] = field_test_mode
        soil_header_table['field_description'] = field_description
        soil_header_table['field_material_origin'] = field_material_origin
        return soil_header_table
        
def form_times(header_data):
        time_data = {}
        timestamps = {}
        header_times = {}
        url_time_base = url_base + 'field_services/clocks/field_services_clocks/'
        for i in range(len(header_data['formseq'])):
                url_time = url_time_base + str(header_data['workorderreqseq'][i])+'/'+str(header_data['siteseq'][i])
                time = urllib2.Request(url_time)
                time = json.loads(urllib2.urlopen(time).read())
                if time != []:
                        for k, v in enumerate(time):
                                header_times[k] = time[v]
                del url_time
                if header_times != {}:
                        for r in header_times.keys():
                                for s in range(len(header_times[r])):
                                        header_times[r][s]['siteseq'] = header_data['siteseq'][i]
                                        header_times[r][s]['workorderreqseq'] = header_data['workorderreqseq'][i]
        if header_times != {}:
                for m in header_times.keys():
                        for n in range(len(header_times[m])):
                                if header_times[m][n]['type'] == 'standby':
                                        if 'duration' in header_times[m][n].keys():
                                                timestamps[header_times[m][n]['type']] = header_times[m][n]['duration']
                                        else:
                                                minutes = header_times[m][n]['minutes']
                                                hours = header_times[m][n]['hours']
                                                if minutes == None:
                                                        minutes = 0
                                                else:
                                                        minutes = float(minutes)
                                                        minutes = round(minutes/60, 2)
                                                if hours == None:
                                                        hours = 0
                                                else:
                                                        hours = float(hours)
                                                timestamps[header_times[m][n]['type']] = hours + minutes
                                                
                                else:
                                        timestamps[header_times[m][n]['type']] = header_times[m][n]['timestamp']
                                timestamps['siteseq'] = header_times[m][n]['siteseq']
                                time_data[header_times[m][n]['workorderreqseq']] = timestamps
                return time_data
        else:
                return  

def update_header_table(header_data, time_data):
        connection  = 'DSN='+config.INPUT_DATA['DSN']+';UID='+config.INPUT_DATA['UID']+';PWD='+config.INPUT_DATA['PWD']
        db_header_table = 'field_services_header'
        header_cnxn = pyodbc.connect(connection)
        header_cursor = header_cnxn.cursor()
        for i in range(len(header_data['formseq'])):
                header_cursor.execute("INSERT INTO "+db_header_table+"(formseq, siteseq, workorderseq, taskseq, form_type, orig_taskseq, status, field_user_seq) values (?, ?, ?, ?, ?, ?, ?, ?)",
                                        (header_data['formseq'][i], header_data['siteseq'][i], header_data['workorderreqseq'][i], header_data['taskseq'][i],
                                         header_data['form_type'][i], header_data['orig_taskseq'][i], header_data['status'][i], str(header_data['user_seq'][i])))
        header_cnxn.commit()
        for i in range(len(header_data['formseq'])):
                if header_data['set_number'][i] != []:
                        header_cursor.execute("UPDATE "+db_header_table+" set number_of_sets = ? where formseq = ?",(header_data['set_number'][i], header_data['formseq'][i]))
                if 'field_weather' in header_data:
                        if header_data['field_weather'][i] != []:
                                header_cursor.execute("UPDATE "+db_header_table+" set field_weather = ? where formseq = ?",(header_data['field_weather'][i], header_data['formseq'][i]))
                if 'field_temperature' in header_data:
                        if header_data['field_temperature'][i] != []:
                                header_cursor.execute("UPDATE "+db_header_table+" set field_temperature = ? where formseq = ?",(header_data['field_temperature'][i], header_data['formseq'][i]))
                if 'field_temperature2' in header_data:
                        if header_data['field_temperature2'][i] != []:
                                header_cursor.execute("UPDATE "+db_header_table+" set field_temperature2 = ? where formseq = ?",(header_data['field_temperature2'][i], header_data['formseq'][i]))                        
        header_cnxn.commit()
        if time_data != None:
                for j in time_data.keys():
                        if 'left for job' in time_data[j]:
                                t_l = float(time_data[j]['left for job'])
                                left_for = dt.fromtimestamp(t_l)
                                header_cursor.execute("UPDATE "+db_header_table+" set left_for = ? where workorderseq = ?",(left_for, j))
                        if 'arrive' in time_data[j]:
                                t_a = float(time_data[j]['arrive'])
                                arrived = dt.fromtimestamp(t_a)
                                header_cursor.execute("UPDATE "+db_header_table+" set arrived = ? where workorderseq = ?",(arrived, j))
                        if 'depart' in time_data[j]:
                                t_d = float(time_data[j]['depart'])
                                departed = dt.fromtimestamp(t_d)
                                header_cursor.execute("UPDATE "+db_header_table+" set departed = ? where workorderseq = ?",(departed, j))
                        if 'standby' in time_data[j]:
                                standby = time_data[j]['standby']
                                if standby != '':
                                        standby = float(standby)
                                        header_cursor.execute("UPDATE "+db_header_table+" set standby_hours = ? where workorderseq = ?",(standby, j))
                header_cnxn.commit()
        header_cnxn.close()

def update_soil_header(header_data):
        connection = 'DSN='+config.INPUT_DATA['DSN']+';UID='+config.INPUT_DATA['UID']+';PWD='+config.INPUT_DATA['PWD']
        db_header_table = 'field_services_header'
        header_cnxn = pyodbc.connect(connection)
        header_cursor = header_cnxn.cursor()
        columns = ['field_guage_date', 'field_remarks', 'field_guage_no', 'field_nbr_specimens', 'field_material_no',
                       'field_guage_ds', 'field_other', 'field_guage_ms', 'field_date_tested', 'field_gauge_make', 'field_gauge_model', 'field_test_mode',
                           'field_description', 'field_material_origin']
        dt_columns = []
        for row in header_cursor.columns(db_header_table):
                if row.column_name in columns:
                        if row.type_name == 'datetime':
                                dt_columns.append(row.column_name)                
        for i in columns:
                for j in range(len(header_data['formseq'])):
                        if i in dt_columns:
                                if header_data[i][j] != []:                              
                                        time_data = float(header_data[i][j])
                                        t = dt.utcfromtimestamp(time_data)
                                        t_new = t.time()
                                        d = t.date()
                                        if d.year == 1970:
                                                d = d.replace(1900)
                                        else:
                                                t = dt.fromtimestamp(time_data)
                                                t_new = t.time()
                                                d = t.date()
                                        time_formatted = dt.combine(d, t_new)
                                        header_data[i][j] = time_formatted
                        if header_data[i][j] != []:
                                header_cursor.execute("UPDATE "+db_header_table+" set "+i+"=? where formseq = ?",(header_data[i][j], header_data['formseq'][j]))
        header_cnxn.commit()
        header_cnxn.close()

def unserialize_field_info(data):
	global tag_data, age_data
	for i in data.keys():
                tag = []
                age = []
                if type(data[i]['field_specimen_info']) is dict:
                        for j in range(len(data[i]['field_specimen_info']['und'])):
                                spec_info = loads(dumps(data[i]['field_specimen_info']['und'][j]))
                                spec_info_str = spec_info['composed']
                                if spec_info_str.split('"')[1] == '':
                                        tag.append('NA')
                                else:
                                        tag.append(spec_info_str.split('"')[1])
                                if spec_info_str.split('"')[3] == '':
                                        age.append('NA')
                                else:
                                        age.append(spec_info_str.split('"')[3])
                	tag_data = ''
                        age_data = ''
                        for k in range(len(tag)):
                                if k == len(tag) - 1:
                                        tag_data = tag_data + str(tag[k])
                                        age_data = age_data + str(age[k])
                                else:
                                        tag_data = tag_data + str(tag[k]) + ', '
                                        age_data = age_data + str(age[k]) + ', '
                        data[i]['field_tag_no'] = tag_data
                        data[i]['field_age_tested'] = age_data
                else:
                        data[i]['field_tag_no'] = ''
                        data[i]['field_age_tested'] = ''
        return data

def set_standard_weather(data):
        for i in data.keys():
                weather = ''
                if type(data[i]['field_sky_conditions']) is dict:
                        weather = data[i]['field_sky_conditions']['und'][0]['value']
                if type(data[i]['field_precipitation']) is dict:
                        if weather != '':
                                weather = weather + '/' + data[i]['field_precipitation']['und'][0]['value']
                        else:
                                weather = data[i]['field_precipitation']['und'][0]['value']
                if type(data[i]['field_wind_conditions']) is dict:
                        if weather != '':
                                weather = weather + '/' + data[i]['field_wind_conditions']['und'][0]['value']
                        else:
                                weather = data[i]['field_wind_conditions']['und'][0]['value']
                if type(data[i]['field_temperature_conditions']) is dict:
                        if weather != '':
                                weather = weather + '/' + data[i]['field_temperature_conditions']['und'][0]['value']
                        else:
                                weather = data[i]['field_temperature_conditions']['und'][0]['value']
                data[i]['field_weather'] = weather
        return data

def set_mtgroup_others(data):
        for i in data.keys():
                if 'field_weather' in data[i].keys():
                        if type(data[i]['field_weather']) is dict:
                                if data[i]['field_weather']['und'][0]['value'] == 'Other':
                                        if type(data[i]['field_weather_other']) is dict:
                                                data[i]['field_weather'] = data[i]['field_weather_other']
                if 'field_sample_at' in data[i].keys():
                        if type(data[i]['field_sample_at']) is dict:
                                if data[i]['field_sample_at']['und'][0]['value'] == 'Other':
                                        if type(data[i]['field_sample_at_other']) is dict:
                                                data[i]['field_sample_at'] = data[i]['field_sample_at_other']
        return data

def set_material_method(data):
        for i in data.keys():
                mortar_method_list = []
                material_check_list = []
                material_string = ''
                method_string = ''
                material_list = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
                method_list = [1, 2, 3, 4, 5]
                if type(data[i]['field_material_checks']) is dict:
                        material_checks = data[i]['field_material_checks']['und']
                        for j in range(len(material_checks)):
                                material_check_list.append(int(material_checks[j]['value']))
                        for k in range(len(material_list)):
                                if k != 11:
                                        if material_list[k] in material_check_list:
                                                char = 'Y'
                                        else:
                                                char = ' '
                                else:
                                        if type(data[i]['field_other_material']) is dict:
                                                char = 'Y' + str(data[i]['field_other_material']['und'][0]['value'])
                                        else:
                                                char = ' '
                                material_string = material_string + char
                data[i]['field_material'] = material_string
                if type(data[i]['field_mortar_method']) is dict:
                        mortar_method = data[i]['field_mortar_method']['und']
                        for j in range(len(mortar_method)):
                                if mortar_method[j]['value'] == 'Other':
                                        mortar_method_list.append(5)
                                else:
                                        mortar_method_list.append(int(mortar_method[j]['value']))
                        for k in range(len(method_list)):
                                if k != 4:
                                        if method_list[k] in mortar_method_list:
                                                char2 = 'Y'
                                        else:
                                                char2 = ' '
                                else:
                                        if type(data[i]['field_other_method']) is dict:
                                                char2 = 'Y' + str(data[i]['field_other_method']['und'][0]['value'])
                                        else:
                                                char2 = ' '
                                method_string = method_string + char2
                data[i]['field_mortar_method'] = method_string
        return data

def set_ole_pier_data(data):
        for i in data.keys():
                if type(data[i]['field_steel_vertical']) is dict:
                        steel = data[i]['field_steel_vertical']['und'][0]['value']
                        if steel.find('Ties') < 0:
                                if steel.find(',') > 0:
                                        steel_values = steel.split(',')
                                        steel_values[0].strip()
                                        steel_values[1].strip()
                                        steel_values[2].strip()
                                        if steel_values[0].isdigit() and steel_values[1].isdigit() and steel_values[2].isdigit():
                                                steel = steel_values[0] + ' #' + steel_values[1] + ' w/ #' + steel_values[2] + ' Ties'
                                        else:
                                                steel = steel
                                elif steel.find(' ') > 0:
                                        steel_values = steel.split(' ')
                                        steel_values[0].strip()
                                        steel_values[1].strip()
                                        steel_values[2].strip()
                                        if steel_values[0].isdigit() and steel_values[1].isdigit() and steel_values[2].isdigit():
                                                steel = steel_values[0] + ' #' + steel_values[1] + ' w/ #' + steel_values[2] + ' Ties'
                                        else:
                                                steel = steel
                                else:
                                        steel = steel
                        else:
                                steel = steel
                        data[i]['field_steel_vertical'] = steel
                if type(data[i]['field_steel_dowels']) is dict:
                        dowels = data[i]['field_steel_dowels']['und'][0]['value']
                        if dowels.find('#') < 0:
                                if dowels.find(',') > 0:
                                        dowel_values = dowels.split(',')
                                        dowel_values[0].strip()
                                        dowel_values[1].strip()
                                        if dowel_values[0].isdigit() and dowel_values[1].isdigit():
                                                dowels = dowel_values[0] + ' #' + dowel_values[1]
                                        else:
                                                dowels = dowels
                                elif dowels.find(' ') > 0:
                                        dowel_values = dowels.split(' ')
                                        dowel_values[0].strip()
                                        dowel_values[1].strip()                                        
                                        if dowel_values[0].isdigit() and dowel_values[1].isdigit():
                                                dowels = dowel_values[0] + ' #' + dowel_values[1]
                                        else:
                                                dowels = dowels
                                else:
                                        dowels = dowels
                        else:
                                dowels = dowels
                        data[i]['field_steel_dowels'] = dowels
        return data
                                        
def update_data_table(data, db_table, table_key, table_key2=None):
        columns = []
        time_data = {}
        time_formatted = {}
        date_data = []
        col_type = []
        column_info = {}
        connection = 'DSN='+config.INPUT_DATA['DSN']+';UID='+config.INPUT_DATA['UID']+';PWD='+config.INPUT_DATA['PWD']
        table_cnxn = pyodbc.connect(connection)
        table_cursor = table_cnxn.cursor()
        for row in table_cursor.columns(db_table):
                if row.column_name[:1] == 'f':
                        columns.append(row.column_name)
                        col_type.append(row.type_name)
        for i in range(len(col_type)):
                column_info[columns[i]] = col_type[i]
        for j in data.keys():
                if table_key2 > '':
                        table_cursor.execute("INSERT INTO " + db_table + "(formseq, " + table_key + ", " + table_key2 + " ) values (?, ?, ?)",(data[j]['formseq'], data[j][table_key], data[j][table_key2]))
                else:
                        table_cursor.execute("INSERT INTO " +db_table + "(formseq, " + table_key + ") values (?, ?)",(data[j]['formseq'], data[j][table_key]))                                             
        table_cnxn.commit()
        for i in columns:
                for j in data.keys():
                        if i in data[j].keys():
                                if type(data[j][i]) is dict:
                                        value = data[j][i]['und'][0]['value']
                                        if column_info[i] == 'varchar' or column_info[i] == 'char':
                                                value = format(value)
                                                ##value = str(value)
                                                if table_key2 > '':
                                                        table_cursor.execute("UPDATE "+db_table+" set "+i+"=? where formseq = ? and "+table_key+" = ? and "+table_key2+" = ?",(value, data[j]['formseq'], data[j][table_key], data[j][table_key2]))
                                                else:
                                                        table_cursor.execute("UPDATE "+db_table+" set "+i+"=? where formseq = ? and "+table_key+" = ?",(value, data[j]['formseq'], data[j][table_key]))
                                        elif column_info[i] != 'datetime':
                                                if i == 'field_bowl_weight' or i == 'field_fresh_concrete_bowl':
                                                        value = round(float(value), 1)
                                                if table_key2 > '':
                                                        table_cursor.execute("UPDATE "+db_table+" set "+i+"=? where formseq = ? and "+table_key+" = ? and "+table_key2+" = ?",(value, data[j]['formseq'], data[j][table_key], data[j][table_key2]))
                                                else:
                                                        table_cursor.execute("UPDATE "+db_table+" set "+i+"=? where formseq = ? and "+table_key+" = ?",(value, data[j]['formseq'], data[j][table_key]))                                                        
                                        elif column_info[i] == 'datetime':
                                                time_data[i] = float(value)
                                                t = dt.utcfromtimestamp(time_data[i])
                                                t_new = t.time()
                                                d = t.date()
                                                if d.year == 1969:
                                                        d = dt.now().date()
                                                        t_new = t_new.replace(0, 0, 0, 0)
                                                if d.year == 1970:
                                                        d = d.replace(1900)
                                                else:
                                                        t = dt.fromtimestamp(time_data[i])
                                                        t_new = t.time()
                                                        d = t.date()
                                                time_formatted[i] = dt.combine(d, t_new)
                                                if table_key2 > '':
                                                        table_cursor.execute("UPDATE "+db_table+" set "+i+"=? where formseq = ? and "+table_key+" = ? and "+table_key2+" = ?",(time_formatted[i], data[j]['formseq'], data[j][table_key], data[j][table_key2]))
                                                else:
                                                        table_cursor.execute("UPDATE "+db_table+" set "+i+"=? where formseq = ? and "+table_key+" = ?",(time_formatted[i], data[j]['formseq'], data[j][table_key]))
                                elif i != 'formseq' and data[j][i] != [] and data[j][i] != '':
                                        if table_key2 > '':
                                                table_cursor.execute("UPDATE "+db_table+" set "+i+"=? where formseq = ? and "+table_key+" = ? and "+table_key2+" = ?",(data[j][i], data[j]['formseq'], data[j][table_key], data[j][table_key2]))
                                        else:
                                                table_cursor.execute("UPDATE "+db_table+" set "+i+"=? where formseq = ? and "+table_key+" = ?",(data[j][i], data[j]['formseq'], data[j][table_key]))
        for j in data.keys():
                if 'field_specimen_size' in data[j].keys():
                        if type(data[j]['field_specimen_size']) is dict:
                                valid_size = True
                                value = data[j]['field_specimen_size']['und'][0]['value']
                                if value == 'Other':
                                        if type(data[j]['field_spec_size_other']) is dict:
                                                value = data[j]['field_spec_size_other']['und'][0]['value']
                                        else:
                                                valid_size = False
                                if valid_size:
                                        size = value.split('x')
                                        diameter = float(size[0])
                                        length = float(size[1])
                                        table_cursor.execute("UPDATE field_services_concrete set field_diameter = ?, field_length = ? where formseq = ? and set_number = ?",
                                                             (diameter, length, data[j]['formseq'], data[j]['set_number']))
        for j in data.keys():
                if 'field_cube_size' in data[j].keys():
                        if type(data[j]['field_cube_size']) is dict:
                                value = data[j]['field_cube_size']['und'][0]['value']
                                size = value.split('x')
                                width = float(size[0])
                                width2 = float(size[1])
                                height = float(size[2])
                                table_cursor.execute("UPDATE field_services_concrete set field_width = ?, field_width2 = ?, field_height = ? where formseq = ? and set_number = ?",
                                                     (width, width2, height, data[j]['formseq'], data[j]['set_number']))
        table_cnxn.commit()
        table_cnxn.close()

def formupdate(field):
        connection = 'DSN='+config.INPUT_DATA['DSN']+';UID='+config.INPUT_DATA['UID']+';PWD='+config.INPUT_DATA['PWD']
        form_table = 'field_services_header'
        form_cnxn = pyodbc.connect(connection)
        form_cursor = form_cnxn.cursor()
        url_index = url_base + 'field_services/md_form/field_services_md_forms.json'
        index = urllib2.Request(url_index)
        index = json.loads(urllib2.urlopen(index).read())
        form = str(config.INPUT_DATA['form'])
        if config.INPUT_DATA['userstatus'] == 3:
                form_cursor.execute('SELECT status_remarks FROM '+form_table+' where formseq = ?' , (config.INPUT_DATA['form']))
                remarks = form_cursor.fetchone()
                if None in remarks:
                        print remarks
        if index == [False]:
                print index
        else:
                update_form = {}
                siteseq = index[form]['master_record']['siteseq']
                workorderreqseq = index[form]['master_record']['workorderreqseq']
                orig_taskseq = index[form]['master_record']['orig_taskseq']
                taskseq = index[form]['master_record']['taskseq']
                url_update = url_base + 'field_services/md_form/field_services_md_forms/'+str(workorderreqseq)+'/'+str(siteseq)+'/'+str(orig_taskseq)+'/'+str(taskseq)
                update_form['status'] = config.INPUT_DATA['userstatus']
                if config.INPUT_DATA['userstatus'] == 3:
                        update_form['comment'] = remarks
                response = requests.put(url_update, update_form)
                if response.status_code == 500 or response.status_code == 404:
                        url_update = url_base + 'field_services/md_form/field_services_md_forms/'+str(workorderreqseq)+'/'+str(siteseq)+'/'+str(orig_taskseq)
                        response = requests.put(url_update, update_form)
                if response.status_code == 500 or response.status_code == 404:
                        url_update = url_base + 'field_services/md_form/field_services_md_forms/'+str(workorderreqseq)+'/'+str(siteseq)+'/'+str(taskseq)
                        response = requests.put(url_update, update_form)
                if response.status_code == 200:
                        form_cursor.execute('UPDATE '+form_table+' set status = ? where formseq = ?', (update_form['status'], config.INPUT_DATA['form']))
                        form_cnxn.commit()
                else:
                        print str(response.status_code) + ' => Status Code Failure'
        form_cnxn.close()

def formreset():
        siteseq = config.INPUT_DATA['siteseq']
        worseq = config.INPUT_DATA['worseq']
        orig_taskseq = config.INPUT_DATA['origtaskseq']
        taskseq = config.INPUT_DATA['taskseq']
        status = config.INPUT_DATA['userstatus']
        connection = 'DSN='+config.INPUT_DATA['DSN']+';UID='+config.INPUT_DATA['UID']+';PWD='+config.INPUT_DATA['PWD']
        form_table = 'field_services_header'
        form_cnxn = pyodbc.connect(connection)
        form_cursor = form_cnxn.cursor()
        url_update = url_base + 'field_services/md_form/field_services_md_forms/' + str(worseq) + '/' + str(siteseq) + '/' + str(orig_taskseq) + '/' + str(taskseq)
        update_form = {}
        update_form['status'] = status
        resp = requests.put(url_update, update_form)
        if resp.status_code == 200:
                form_cursor.execute("SELECT formseq FROM " + form_table + " WHERE siteseq = ? and workorderseq = ? and orig_taskseq = ? and taskseq = ?", (siteseq, worseq, orig_taskseq, taskseq))
                formid = form_cursor.fetchone()
                if formid == None:
                        form_cnxn.close()
                        print 'Form reset. Nothing to update in database'
                else:
                        form = formid[0]
                        if update_form['status'] == 1:
                                update_form['status'] = 3
                        form_cursor.execute('UPDATE '+form_table+' set status = ? where formseq = ?', (update_form['status'], form))
                        form_cnxn.commit()
                        form_cnxn.close()
                        print 'Everything is reset'
        else:
                form_cnxn.close()
                print 'HTTP Error Code: ' + str(resp.status_code)
                
def accepttimes():
        ## Write status file and update form_submitted in workorderrequest when field form has been submitted
        connection = 'DSN='+config.INPUT_DATA['DSN']+';UID='+config.INPUT_DATA['UID']+';PWD='+config.INPUT_DATA['PWD']
        form_table = 'workorderrequest'
        form_cnxn = pyodbc.connect(connection)
        form_cursor = form_cnxn.cursor()
        status_path = gs_dir_path+gs_db_path+'\\export\\'+config.INPUT_DATA['userpath']+'\\status.txt' 
        dsptch_status = open(status_path, 'w')
        wor_status = 'T'
        form_submitted = 'Y'
        url_index = url_base + 'field_services/md_form/field_services_md_forms.json'
        index = urllib2.Request(url_index)
        index = json.loads(urllib2.urlopen(index).read())
        if index != [False]:
                form_data = {}
                for j, v in enumerate(index.keys()):
                        form_data[j] = index[v]['master_record']
                for i in form_data.keys():
                        dsptch_status.write('{0}\t{1}\t{2}\n'.format(form_data[i]['siteseq'],str(form_data[i]['workorderreqseq']),wor_status))
                        form_cursor.execute('UPDATE ' + form_table + ' SET form_submitted = ? WHERE siteseq = ? and workorderreqseq = ?', (form_submitted, form_data[i]['siteseq'], form_data[i]['workorderreqseq']))
                        form_cnxn.commit()
        dsptch_status.close()
        form_cnxn.close()
        ## Write pickup locations for concrete forms where available
        pickup_path = gs_dir_path+gs_db_path+'\\export\\'+config.INPUT_DATA['userpath']+'\\pickup_loc.txt'
        pickup_loc = open(pickup_path, 'w')
        if index != [False]:
                form_master = {}
                form_detail_index = {}
                form_detail = {}
                form_detail_enum = {}
                for j, v in enumerate(index.keys()):
                        form_master[j] = index[v]['master_record']
                        form_detail_index[j] = index[v]['detail_records']
                for i in form_detail_index.keys():
                        for j in form_detail_index[i].keys():
                                form_detail[j] = form_detail_index[i][j]
                for j, v in enumerate(form_detail.keys()):
                        form_detail_enum[j] = form_detail[v]
                for i in form_master.keys():
                        formseq = form_master[i]['id']
                        siteseq = form_master[i]['siteseq']
                        worseq = form_master[i]['workorderreqseq']
                        taskseq = form_master[i]['taskseq']
                        sets = len(form_detail_index[i])
                        for j in form_detail_enum.keys():
                                if form_detail_enum[j]['master_id'] == formseq:
                                        form_detail_enum[j]['siteseq'] = siteseq
                                        form_detail_enum[j]['workorderreqseq'] = worseq
                                        form_detail_enum[j]['taskseq'] = taskseq
                                        form_detail_enum[j]['sets'] = int(sets)
                for i in form_detail_enum.keys():
                        if 'field_pickup_location' in form_detail_enum[i].keys():
                                if type(form_detail_enum[i]['field_pickup_location']) is dict:
                                        pickup = str(form_detail_enum[i]['field_pickup_location']['und'][0]['value'])
                                else:
                                        pickup = ''
                                set_number = int(form_detail_enum[i]['id'])
                                pickup_loc.write('{0}\t{1}\t{2}\t{3}\t{4}\t{5}\n'.format(form_detail_enum[i]['siteseq'],str(form_detail_enum[i]['workorderreqseq']),set_number,pickup,form_detail_enum[i]['sets'],form_detail_enum[i]['taskseq']))
        pickup_loc.close()
        ## Write accept times
        time_path = gs_dir_path+gs_db_path+'\\export\\'+config.INPUT_DATA['userpath']+'\\tech_accept.txt' 
        tech_accept = open(time_path, 'w')        
        accept_times = {}
        accept_data = {}
        accept_timestamps = {}
        timestamp = []
        worseq = []
        site = []
        emp_email = []
        start_date_str = config.INPUT_DATA['timestamp'].replace('[]',' ')
        start_date = time.mktime(dt.strptime(start_date_str, '%m/%d/%Y %H:%M:%S').timetuple())
        url_accept = url_base+'field_services/clocks/field_services_clocks?parameters[type]=accept&parameters[start_date]='+str(start_date)
        accept = urllib2.Request(url_accept)
        accept = json.loads(urllib2.urlopen(accept).read())
        if accept != []:
                for k, v in enumerate(accept):
                        accept_times[k] = accept[v]
                for i in accept_times.keys():
                        for j in range(len(accept_times[i])):
                                time_formatted = dt.fromtimestamp(float(accept_times[i][j]['timestamp']))
                                timestamp.append(time_formatted)
                                worseq.append(accept_times[i][j]['workorderreqseq'])
                                site.append(accept_times[i][j]['siteseq'])
                                emp_email.append(accept_times[i][j]['mail'])
                accept_timestamps['accept'] = timestamp
                accept_timestamps['workorderreqseq'] = worseq
                accept_timestamps['siteseq'] = site
                accept_timestamps['email'] = emp_email
                for i in range(len(accept_timestamps['accept'])):
                        tech_accept.write('{0}\t{1}\t{2}\t{3}\n'.format(accept_timestamps['siteseq'][i],str(accept_timestamps['workorderreqseq'][i]),accept_timestamps['accept'][i],accept_timestamps['email'][i]))
        tech_accept.close()
        
        
        

if __name__ == "__main__":
	if len(sys.argv) > 1:
		arguments = sys.argv
		pb_cmd.create_var_dictionary( arguments )
		connection = 'DSN=lims_list;UID=sa;PWD=sa'
                cnxn = pyodbc.connect(connection)
                cursor = cnxn.cursor()
                cursor.execute('SELECT field_url, field_pwd FROM company_db WHERE companyseq = ?', config.INPUT_DATA['companyseq'])
                row = cursor.fetchall()
                url_base = row[0][0]
                field_pass = row[0][1]
                cnxn.close()
    #            url_login = url_base + 'field_services/user/user/login'
    #            url_token = url_base + 'services/session/token'
    #            data = {'name':'elmtree admin','pass':field_pass}
    #            data_encoded = urllib.urlencode(data)
    #            request = urllib2.Request(url_login, data_encoded)
    #            response = json.loads(urllib2.urlopen(request).read())
    #            headers = {}
    #            session_name = response['session_name']
    #            session_id = response['sessid']
    #            headers['Cookie'] = session_name + '=' + session_id
    #            token = urllib2.Request(url_token, None, headers)
    #            token  = urllib2.urlopen(token).read()
    #            headers['X-CSRF-Token'] = token
                curr_db = config.INPUT_DATA['DSN']
                gs_db = curr_db.split('_')
                gs_db_path = gs_db[1]
                if gs_db_path == 'd&s':
                        gs_db_path = 'dands'
  		if config.INPUT_DATA['command'][-6:] == 'update':
                        command = config.INPUT_DATA['command'] + '(' + str(config.INPUT_DATA['field']) + ')'
                elif config.INPUT_DATA['command'] == 'usercreate':
                        command = config.INPUT_DATA['command'] + '(1)'
                else:        
                        command = config.INPUT_DATA['command'] + '()'
		eval(command)
	else:
		arguments = r"-iD_command_c2s=formsindex -iD_firstname_c2s='' -iD_lastname_c2s='' -iD_email_c2s='' -iD_pass_c2s='' -iD_DSN_c2s='lims_test' -iD_UID_c2s=sa -iD_PWD_c2s=sa -iD_userstatus_i2i=0 -iD_newfirst_c2s='' -iD_newlast_c2s='' -iD_newemail_c2s='' -iD_newpass_c2s='' -iD_field_i2i=1 -iD_companyseq_i2i=2 -iD_form_i2i=0"
		arguments = arguments.split(" ")
		pb_cmd.create_var_dictionary( arguments )
		connection = 'DSN=lims_list;UID=sa;PWD=sa'
                cnxn = pyodbc.connect(connection)
                cursor = cnxn.cursor()
                cursor.execute('SELECT field_url, field_pwd FROM company_db WHERE companyseq = ?', config.INPUT_DATA['companyseq'])
                row = cursor.fetchall()
                url_base = row[0][0]
                field_pass = row[0][1]
                cnxn.close()
                url_login = url_base + 'field_services/user/user/login'
                url_token = url_base + 'services/session/token'
                data = {'name':'elmtree admin','pass':field_pass}
                data_encoded = urllib.urlencode(data)
                request = urllib2.Request(url_login, data_encoded)
                response = json.loads(urllib2.urlopen(request).read())
                headers = {}
                session_name = response['session_name']
                session_id = response['sessid']
                headers['Cookie'] = session_name + '=' + session_id
                token = urllib2.Request(url_token, None, headers)
                token  = urllib2.urlopen(token).read()
                headers['X-CSRF-Token'] = token
		if config.INPUT_DATA['command'][-6:] == 'update':
                        command = config.INPUT_DATA['command'] + '(' + str(config.INPUT_DATA['field']) + ')'
                else:        
                        command = config.INPUT_DATA['command'] + '()'
		eval(command)
	app = wx.PySimpleApp(0)
	app.MainLoop()

	
		

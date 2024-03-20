import os

'''
Note:
- Sheet for excel needs to be named "weekly" or "monthly"
- Tables within sheet need to be named "campaign", "download", "time". Do this from "design" tab

Buckets:
- samsung.ads.data.share
- dev-samsung-dm-data-share-paramount-plus
- prod-samsung-dm-data-share-paramount-plus
- dev-samsung-dm-data-share-pluto
- prod-samsung-dm-data-share-pluto

* when udw_stage key is present, bucket and prefix will be modified with app_temp_storage value
'''


def get_app_config():
    return {
        'path': os.path.realpath(os.path.dirname(__file__)) + '\\'
        ,'udw_stage_keys': {
            'udw_test':         {'stage': '@adbiz_data.samsung_ads_data_share/analytics/custom/vaughn/test/python/udw/'} 
            ,'udw_pplus_na':    {'stage': '@udw_marketing_analytics_reports.paramount_plus_external_us/'} 
            ,'udw_pplus_int':   {'stage': '@udw_marketing_analytics_reports.paramount_plus_external_international/'} 
            ,'udw_pluto_na':    {'stage': '@udw_marketing_analytics_reports.pluto_external_us/'} 
            ,'udw_pluto_int':   {'stage': '@udw_marketing_analytics_reports.pluto_external_international/'} 
        }
        ,'destination_bucket_keys': {
            'app_temp_storage': {'bucket': 'samsung.ads.data.share', 'prefix': 'analytics/custom/vaughn/temp_storage/', 'udw_stage': ''}
            ,'dev_test':        {'bucket': 'samsung.ads.data.share', 'prefix': 'analytics/custom/vaughn/test/python/', 'udw_stage': ''}
            ,'prod_test':       {'bucket': 'foo.bar', 'prefix': 'pplus_pluto/', 'udw_stage': 'udw_test'}
            ,'dev_pplus_na':    {'bucket': 'dev-samsung-dm-data-share-paramount-plus', 'prefix': 'paramount_plus_us/', 'udw_stage': ''}
            ,'dev_pplus_int':   {'bucket': 'dev-samsung-dm-data-share-paramount-plus', 'prefix': 'paramount-plus-international/', 'udw_stage': ''}
            ,'prod_pplus_na':   {'bucket': 'prod-samsung-dm-data-share-paramount-plus', 'prefix': 'paramount_plus_us/', 'udw_stage': 'udw_pplus_na'}
            ,'prod_pplus_int':  {'bucket': 'prod-samsung-dm-data-share-paramount-plus', 'prefix': 'paramount-plus-international/', 'udw_stage': 'udw_pplus_int'}
            ,'dev_pluto_na':    {'bucket': 'dev-samsung-dm-data-share-pluto', 'prefix': 'pluto-us/', 'udw_stage': ''}
            ,'dev_pluto_int':   {'bucket': 'dev-samsung-dm-data-share-pluto', 'prefix': 'pluto-international/', 'udw_stage': ''}
            ,'prod_pluto_na':   {'bucket': 'prod-samsung-dm-data-share-pluto', 'prefix': 'pluto-us/', 'udw_stage': 'udw_pluto_na'}
            ,'prod_pluto_int':  {'bucket': 'prod-samsung-dm-data-share-pluto', 'prefix': 'pluto-international/', 'udw_stage': 'udw_pluto_int'}
        }
    }


def get_report_config():
    return {
        'start_date': '2024-02-19'
        ,'end_date': '2024-02-25'
        ,'report_execution_date': '2024-02-26'
        ,'reports':[
            # {
            #     'partner': 'paramount_plus'                       # paramount_plus | pluto
            #     ,'bucket_key': 'dev_test'                             # see 'destination_bucket_keys' dictionary above for options
            #     ,'interval': 'weekly'                             # weekly | monthly
            #     ,'region': 'au'                                   # e.g., fr | es | de | it
            #     ,'filename': 'input\\test_files\\test_pplus_au.xlsx'     # file name or path relative to this directory
            # }
            # --------------------------------------
            # paramount+
            # --------------------------------------
            {
                'partner': 'paramount_plus'
                ,'interval': 'weekly'
                ,'bucket_key': 'prod_test' 
                ,'region': 'au'
                ,'filename': 'input\\test_files\\test_pplus_au.xlsx'
            }
            ,{
                'partner': 'paramount_plus'
                ,'interval': 'weekly'
                ,'bucket_key': 'prod_test' 
                ,'region': 'fr'
                ,'filename': 'input\\test_files\\test_pplus_fr.xlsx'
            }  
            # --------------------------------------
            # pluto
            # --------------------------------------
            ,{
                'partner': 'pluto'
                ,'interval': 'weekly'
                ,'bucket_key': 'prod_test' 
                ,'region': 'de'
                ,'filename': 'input\\test_files\\test_pluto_de.xlsx'
            }
            ,{
                'partner': 'pluto'
                ,'interval': 'weekly'
                ,'bucket_key': 'prod_test'
                ,'region': 'fr'
                ,'filename': 'input\\test_files\\test_pluto_fr.xlsx'
            }
            # --------------------------------------
        ]
    }


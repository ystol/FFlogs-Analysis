import json
import requests
import pandas as pd
import csv
from Functions import General_Functions as gFunc


def fflogs_getReports(reports_url):
    get_reports_call = requests.get(reports_url).json()

    reports = []
    for eachentry in get_reports_call:
        eachentry['date'], eachentry['day'], eachentry['start_time'] = gFunc.convtimestamp(eachentry['start'] / 1000)
        ignore, ignore, eachentry['end_time'] = gFunc.convtimestamp(eachentry['end'] / 1000)
        reports.append(eachentry)

    allreports = pd.DataFrame.from_dict(reports)
    allreports.rename(columns={'id': 'reportid'}, inplace=True)
    return allreports


def fflogs_getfightdata(report_list, prefix_url, suffix_url, owner):
    fights = []
    report_length = len(report_list)
    progress = 1
    for eachreport in report_list:
        fights_url = prefix_url + eachreport + suffix_url
        print("\r" + "Gathering data: " + str(progress) + "/" + str(report_length) + " from url: " + fights_url, end="")
        get_fights_call = requests.get(fights_url).json()
        allfights = get_fights_call['fights']

        for eachfight in allfights:
            fight_headers = ['id', 'boss', 'name', 'zoneName', 'kill', 'bossPercentage', 'fightPercentage',
                             'lastPhaseForPercentageDisplay']
            try:
                values = [eachfight[header] for header in fight_headers]
                fight_length = gFunc.TimeDifference(eachfight['start_time'], eachfight['end_time']).minutes_raw
                values.append(fight_length)
            except KeyError:
                # ignoring for now, but an extra attempt is being found here
                continue
            values.insert(0, eachreport)
            values.insert(1, owner)
            sql_headers = ['reportid', 'owner', 'run_num', 'boss', 'bossname', 'zone_name', 'defeated', 'bosspercent',
                           'fightpercent', 'lastphase', 'fight_length_min']
            fight_data = dict(zip(sql_headers, values))
            fights.append(fight_data)
        progress += 1
    print('\n')
    print('Fight data gathering complete.')
    return pd.DataFrame.from_dict(fights)


def fflogs_getcharacters(report_list, prefix_url, suffix_url, owner):
    players = []
    report_length = len(report_list)
    progress = 1
    for eachreport in report_list:
        fights_url = prefix_url + eachreport + suffix_url
        print("\r" + "Gathering character data: " + str(progress) + "/" + str(report_length) + " from url: " + fights_url, end="")
        get_fights_call = requests.get(fights_url).json()
        allfights = get_fights_call['friendlies']
        sql_headers = ['charname', 'server', 'static_name', 'firstreport', 'reportowner']
        if len(allfights) > 0:
            for eachfight in allfights:
                fight_headers = ['name', 'server']
                try:
                    values = [eachfight[header] for header in fight_headers]
                except KeyError:
                    # ignoring for now, but an extra attempt is being found sometimes
                    values = ['None', 'None']
                values.insert(len(values), owner+' Pug')
                values.insert(len(values), eachreport)
                values.insert(len(values), owner)
                player_info = dict(zip(sql_headers, values))
                players.append(player_info)
        else:
            values = ['None', 'None', 'None']
            values.insert(len(values), eachreport)
            values.insert(len(values), owner)
            player_info = dict(zip(sql_headers, values))
            players.append(player_info)
        progress += 1
    characters_df = pd.DataFrame.from_dict(players)
    # have to lower all names to remove capitalization issues of same character names
    lowerchars = characters_df.copy()
    lowerchars['charname'] = lowerchars['charname'].str.lower()
    lowerchars.drop_duplicates(subset=['charname'], inplace=True)
    indicies = lowerchars.index
    print('\n')
    print('Character data gathering complete.')
    return characters_df.loc[indicies]


def fflogs_getparticipants(report_list, prefix_url, suffix_url, owner='Defai'):
    participants = []
    report_length = len(report_list)
    progress = 1
    for eachreport in report_list:
        fights_url = prefix_url + eachreport + suffix_url
        print("\r" + "Gathering character data: " + str(progress) + "/" + str(report_length) + " from url: " + fights_url, end="")
        get_fights_call = requests.get(fights_url).json()
        allpeople = get_fights_call['friendlies']
        for eachperson in allpeople:
            try:
                person_fights = eachperson['fights']
                for eachfight in person_fights:
                    sql_headers = ['reportid', 'run_num', 'charname', 'server_name', 'reportowner']
                    person = [eachreport, eachfight['id'], eachperson['name'], eachperson['server'], owner]
                    participant_data = dict(zip(sql_headers, person))
                    participants.append(participant_data)
            except KeyError:
                continue
        progress += 1
    print('\n')
    print('Participant data gathering complete.')
    return pd.DataFrame.from_dict(participants)


def fflogs_getzones(zoneurl):
    get_reports_call = requests.get(zoneurl).json()
    zone_data = []
    zone_columns = ['zone_id', 'zone_name', 'enc_boss_id', 'boss_name']
    for allzones in get_reports_call:
        zone_id = allzones['id']
        zone_name = allzones['name']
        for eachencounter in allzones['encounters']:
            encounter_id = eachencounter['id']
            encounter_name = eachencounter['name']
            row_info = [zone_id, zone_name, encounter_id, encounter_name]
            zone_dict = dict(zip(zone_columns, row_info))
            zone_data.append(zone_dict)
    return pd.DataFrame.from_dict(zone_data)

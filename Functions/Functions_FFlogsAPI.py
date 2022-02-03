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
    for eachreport in report_list:
        fights_url = prefix_url + eachreport + suffix_url
        print(fights_url)
        get_fights_call = requests.get(fights_url).json()
        allfights = get_fights_call['fights']

        for eachfight in allfights:
            fight_headers = ['id', 'boss', 'name', 'zoneID', 'kill', 'bossPercentage', 'fightPercentage',
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
            sql_headers = ['reportid', 'owner', 'run_num', 'boss', 'bossname', 'zoneID', 'defeated', 'bosspercent',
                           'fightpercent', 'lastphase', 'fight_length_min']
            fight_data = dict(zip(sql_headers, values))
            fights.append(fight_data)

    return pd.DataFrame.from_dict(fights)


def fflogs_getcharacters(report_list, prefix_url, suffix_url, owner):
    players = []
    for eachreport in report_list:
        fights_url = prefix_url + eachreport + suffix_url
        print(fights_url)
        get_fights_call = requests.get(fights_url).json()
        allfights = get_fights_call['friendlies']
        for eachfight in allfights:
            fight_headers = ['name', 'server']
            try:
                values = [eachfight[header] for header in fight_headers]
            except KeyError:
                # ignoring for now, but an extra attempt is being found sometimes
                continue
            sql_headers = ['charname', 'server', 'firstreport', 'reportowner']
            values.insert(len(values), eachreport)
            values.insert(len(values), owner)
            player_info = dict(zip(sql_headers, values))
            players.append(player_info)
    characters_df = pd.DataFrame.from_dict(players)
    # have to lower all names to remove capitalization issues of same character names
    lowerchars = characters_df.copy()
    lowerchars['charname'] = lowerchars['charname'].str.lower()
    lowerchars.drop_duplicates(subset=['charname'], inplace=True)
    indicies = lowerchars.index

    return characters_df.loc[indicies]


def fflogs_getparticipants(report_list, prefix_url, suffix_url):
    participants = []
    for eachreport in report_list:
        fights_url = prefix_url + eachreport + suffix_url
        print(fights_url)
        get_fights_call = requests.get(fights_url).json()
        allpeople = get_fights_call['friendlies']
        for eachperson in allpeople:
            try:
                person_fights = eachperson['fights']
                for eachfight in person_fights:
                    sql_headers = ['reportid', 'run_num', 'charname', 'server_name']
                    person = [eachreport, eachfight['id'], eachperson['name'], eachperson['server']]
                    participant_data = dict(zip(sql_headers, person))
                    participants.append(participant_data)
            except KeyError:
                continue

    return pd.DataFrame.from_dict(participants)

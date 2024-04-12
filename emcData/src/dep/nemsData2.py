# %% [markdown]
# # EMC Data Client
#
# ## Usage
#
# For **CorpWebSiteDataReports**, directly call `getCorp()`.
#
# For **MCR Reports**, call `getMCR001()` first to get `["MCRId", "FirstDate", "LastDate", "LoadScenario"]`. Then call `getMCRReport()` to get actual data of the specific report.

# %%
# Global runtime control

# 'FUNCTEST' or 'API':
#   'FUNCTEST' for functional testing
#   'API' for actual API calls
import time
import html
import xml.etree.ElementTree as ET
import pandas as pd
import requests
RUNTIME = 'API'
DATADIR = '/home/sdc/emcData/data/'
TESTDATE = '05-Aug-2023'

# %% [markdown]
# ## Connection Parameters

# %% [markdown]
# Request Heading

# %%
headers = {
    'Content-Type': 'text/xml',
    'Accept-Charset': 'UTF-8',
    'Authorization': 'Basic Y2hlZWtlb25nYW5nOlNEQ3NkYzEyMzQ='
}

# %% [markdown]
# ## Data Fetching

# %%

# Define the maximum number of retries
max_retries = 1
retry_count = 0

# %%


def emcRequest(url, data):
    return requests.request("POST", url, headers=headers, data=data, cert='/home/sdc/emcData/cert/nems2024.pem', verify=False)

# %%


def getCorp(date):
    url = "https://www.emc.nemsdatasvc.wsi.emcsg.com:9534/nemsdsvc/CorpWebSiteDataReports"

    payload = f"""
        <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:cor="http://com/emc/nems/wsd/webservices/reports/corpdata" xmlns:java="java:com.emc.nems.wsd.ui.beans.reports">
        <soapenv:Header/>
        <soapenv:Body>
            <cor:RealTimePriceDataWebService>
                <cor:reportBean>
                    <!--Zero or more repetitions:-->
                    <java:ReportBean>
                        <java:ParamName>Date</java:ParamName>
                        <java:ParamValue>{date}</java:ParamValue>
                    </java:ReportBean>
                </cor:reportBean>
            </cor:RealTimePriceDataWebService>
        </soapenv:Body>
        </soapenv:Envelope>
        """

    # Handle xml response
    res = emcRequest(url=url, data=payload)
    data = res.text

    if RUNTIME == 'FUNCTEST':
        with open(f"{DATADIR}Corp.xml", 'w') as f:
            f.write(data)

    '''
    Parse the SOAP response
    '''
    root = ET.fromstring(data)
    namespaces = {
        'env': 'http://schemas.xmlsoap.org/soap/envelope/',
        'm': 'http://com/emc/nems/wsd/webservices/reports/corpdata'
    }

    try:
        # Extract the embedded XML from <m:return> and unescape it
        embedded_xml_str = root.find('.//m:return', namespaces).text
        embedded_xml_str = html.unescape(embedded_xml_str)
        embedded_root = ET.fromstring(embedded_xml_str)

        data = []
        for report in embedded_root.findall(f'.//RealTimePrice'):
            row = {}
            for elem in report:
                row[elem.tag] = elem.text
            data.append(row)

        corpDf = pd.DataFrame(data)

        corpDf['tradingDate'] = pd.to_datetime(
            corpDf['tradingDate'], format="%d-%b-%Y")

        nameTrans = {
            'period': 'Period',
            'tradingDate': 'Date',
            'demand': 'Demand',
            'tcl': 'TCL',
            'lcp': 'LCP',
            'regulation': 'Regulation',
            'primaryReserve': 'PrimaryReserve',
            'contingencyReserve': 'ContingencyReserve',
            'eheur': 'EHEUR',
            'solar': 'Solar'
        }
        corpDf.rename(columns=nameTrans, inplace=True, errors='ignore')

        if 'secondaryReserve' in corpDf:
            corpDf.drop(columns='secondaryReserve', inplace=True)

        # corpDF should have 72 rows (24 + 48 periods)
        return corpDf

    except:
        print(data)
        error_xml_str = root.find('.//faultstring').text
        return error_xml_str


if RUNTIME == 'FUNCTEST':
    corpDf = getCorp(TESTDATE)
    if corpDf is not None:
        print(corpDf.iloc[0])

# %%


def getMCR001(date, loadScenario, runType='DPR'):
    '''
    Return: 
        mcrDf = [
            [<MCRId>, <FirstDate>, <FirstPeriod>, <LastDate>, <LastPeriod>, <LoadScenario>],
            ...
        ]
    '''
    url = "https://www.emc.nemsdatasvc.wsi.emcsg.com:9534/nemsdsvc/MCRReports"

    payload = f"""
        <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:mpap="http://com/emc/nems/wsd/webservices/reports/mpapi" xmlns:java="java:com.emc.nems.wsd.ui.beans.reports">
        <soapenv:Header/>
        <soapenv:Body>
            <mpap:getMCR001>
                <mpap:reportBean>
                    <!--Zero or more repetitions:-->
                    <java:ReportBean>
                    <java:ParamName>reportID</java:ParamName>
                    <java:ParamValue>MCR001</java:ParamValue>
                    </java:ReportBean>

                    <java:ReportBean>
                    <java:ParamName>ResultDate</java:ParamName>
                    <java:ParamValue>{date}</java:ParamValue>
                    </java:ReportBean>

                    <java:ReportBean>
                    <java:ParamName>RunType</java:ParamName>
                    <java:ParamValue>{runType}</java:ParamValue>
                    </java:ReportBean>

                    <java:ReportBean>
                    <java:ParamName>LoadScenario</java:ParamName>
                    <java:ParamValue>{loadScenario}</java:ParamValue>
                    </java:ReportBean>
                </mpap:reportBean>
            </mpap:getMCR001>
        </soapenv:Body>
        </soapenv:Envelope>
        """

    res = emcRequest(url=url, data=payload)
    data = res.text

    if RUNTIME == 'FUNCTEST':
        with open(f"{DATADIR}MCR001.xml", 'w+') as f:
            f.write(data)

    # Parse the SOAP response
    root = ET.fromstring(data)

    # Namespace map
    namespaces = {
        'env': 'http://schemas.xmlsoap.org/soap/envelope/',
        'm': 'http://com/emc/nems/wsd/webservices/reports/mpapi'
    }

    try:
        # Extract the embedded XML from <m:return> and unescape it
        embedded_xml_str = root.find('.//m:return', namespaces).text
        embedded_xml_str = html.unescape(embedded_xml_str)
        embedded_root = ET.fromstring(embedded_xml_str)

        data = []
        for report in embedded_root.findall(f'.//MCR001Report'):
            row = {}
            for elem in report:
                row[elem.tag] = elem.text
            data.append(row)

        mcrDf = pd.DataFrame(data)

        return mcrDf

    except:
        error_xml_str = root.find('.//faultstring').text
        return error_xml_str


if RUNTIME == 'FUNCTEST':
    mcrDf = getMCR001(TESTDATE, 'M')
    print(mcrDf.iloc[0])

# %%


def getMCRReport(reportName, mcrSerie):

    url = "https://www.emc.nemsdatasvc.wsi.emcsg.com:9534/nemsdsvc/MCRReports"

    payload = f"""
        <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:mpap="http://com/emc/nems/wsd/webservices/reports/mpapi" xmlns:java="java:com.emc.nems.wsd.ui.beans.reports">
        <soapenv:Header/>
        <soapenv:Body>
            <mpap:getMCRReport>
                <mpap:reportBean>
                    <!--Zero or more repetitions:-->
                    <java:ReportBean>
                    <java:ParamName>reportID</java:ParamName>
                    <java:ParamValue>{reportName}</java:ParamValue>
                    </java:ReportBean>

                    <java:ReportBean>
                    <java:ParamName>MCRId</java:ParamName>
                    <java:ParamValue>{mcrSerie['MCRId']}</java:ParamValue>
                    </java:ReportBean>

                    <java:ReportBean>
                    <java:ParamName>FirstDate</java:ParamName>
                    <java:ParamValue>{mcrSerie['FirstDate']}</java:ParamValue>
                    </java:ReportBean>

                    <java:ReportBean>
                    <java:ParamName>LastDate</java:ParamName>
                    <java:ParamValue>{mcrSerie['LastDate']}</java:ParamValue>
                    </java:ReportBean>
                </mpap:reportBean>
            </mpap:getMCRReport>
        </soapenv:Body>
        </soapenv:Envelope>
        """

    res = emcRequest(url=url, data=payload)
    data = res.text

    if RUNTIME == 'FUNCTEST':
        with open(f"{DATADIR}{reportName}.xml", 'w+') as f:
            f.write(data)

    '''
    Parse the SOAP response
    '''
    root = ET.fromstring(data)
    namespaces = {
        'env': 'http://schemas.xmlsoap.org/soap/envelope/',
        'm': 'http://com/emc/nems/wsd/webservices/reports/mpapi'
    }

    try:

        # Extract the embedded XML from <m:return> and unescape it
        embedded_xml_str = root.find('.//m:return', namespaces).text
        embedded_xml_str = html.unescape(embedded_xml_str)
        embedded_root = ET.fromstring(embedded_xml_str)

        data = []
        for report in embedded_root.findall(f'.//{reportName}Report'):
            row = {}
            for elem in report:
                row[elem.tag] = elem.text
            data.append(row)

        mcrReportDf = pd.DataFrame(data)

        nameTrans = {
            "ResultDate": "ForecastDate",
            "ResultPeriod": "ForecastPeriod",
            "TotalLoadMW": "TotalLoad",
            "TotalCurtailedLoad": "TCL",
            "RegulatoryLoadQuantity": "RLQ",
            "UniformSingaporeEnergyPrice": "USEP",
            "CounterfactualUniformSingaporeEnergyPrice": "CUSEP",
            "LoadCurtailmentPrice": "LCP",
            "EnergyShortfallMW": "EnergyShortfall",
            "TotalTransmissionLossMW": "TransmissionLoss",
            "EstimatesHourlyEnergyUpliftRebate": "EHEUR",
            "SolarMW": "Solar"
        }

        mcrReportDf.rename(columns=nameTrans, inplace=True, errors='ignore')

        mcrReportDf['ForecastDate'] = pd.to_datetime(
            mcrReportDf['ForecastDate'], format="%d-%b-%Y")

        if "MCRID" in mcrReportDf.columns:
            mcrReportDf.drop(columns=["MCRID"], inplace=True)

        return mcrReportDf

    except:
        error_xml_str = root.find('.//faultstring').text
        return error_xml_str


if RUNTIME == 'FUNCTEST':
    for idx, mcrSerie in mcrDf.iterrows():
        # print(mcrSerie)
        mcrReportDf = getMCRReport('MCR010', mcrSerie)
        print(mcrReportDf.iloc[0])
        break

# %%
if __name__ == '__main__':

    corpDf = getCorp(TESTDATE)
    corpDf.to_csv(f'Corp.csv', index=False)

    mcrDf = getMCR001(TESTDATE, 'M')
    mcr010 = getMCRReport('MCR010', mcrDf.iloc[0])
    mcr012 = getMCRReport('MCR012', mcrDf.iloc[0])

    mcr010.to_csv(f'{DATADIR}MCR010.csv', index=False)
    mcr012.to_csv(f'{DATADIR}MCR012.csv', index=False)


# %%

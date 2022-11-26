#!/usr/bin/env python3

import csv
import datetime
from itertools import groupby

PATIENT_DATE_FIELD = "CLM_FROM_DT"
PATIENT_ID_FIELD = "DESYNPUF_ID"
PRESCRIPTION_DATE_FIELD = "SRVC_DT"
NDC_ID_FIELD = "PROD_SRVC_ID"
DIABETES_INDEX_DATE = "DIABETES_INDEX_DATE"

inpatientDiagnosisColNames = ["ADMTNG_ICD9_DGNS_CD", "ICD9_DGNS_CD_1", "ICD9_DGNS_CD_2", "ICD9_DGNS_CD_3",
                              "ICD9_DGNS_CD_4", "ICD9_DGNS_CD_5", "ICD9_DGNS_CD_6", "ICD9_DGNS_CD_7",
                              "ICD9_DGNS_CD_8", "ICD9_DGNS_CD_9", "ICD9_DGNS_CD_10"]
outpatientDiagnosisColNames = ["ADMTNG_ICD9_DGNS_CD", "ICD9_DGNS_CD_1", "ICD9_DGNS_CD_2", "ICD9_DGNS_CD_3",
                               "ICD9_DGNS_CD_4", "ICD9_DGNS_CD_5", "ICD9_DGNS_CD_6", "ICD9_DGNS_CD_7",
                               "ICD9_DGNS_CD_8", "ICD9_DGNS_CD_9", "ICD9_DGNS_CD_10"]
carrierDiagnosisColNames = ["ICD9_DGNS_CD_1", "ICD9_DGNS_CD_2", "ICD9_DGNS_CD_3", "ICD9_DGNS_CD_4",
                            "ICD9_DGNS_CD_5", "ICD9_DGNS_CD_6", "ICD9_DGNS_CD_7", "ICD9_DGNS_CD_8",
                            "LINE_ICD9_DGNS_CD_1", "LINE_ICD9_DGNS_CD_2", "LINE_ICD9_DGNS_CD_3",
                            "LINE_ICD9_DGNS_CD_4", "LINE_ICD9_DGNS_CD_5", "LINE_ICD9_DGNS_CD_6",
                            "LINE_ICD9_DGNS_CD_7", "LINE_ICD9_DGNS_CD_8", "LINE_ICD9_DGNS_CD_9",
                            "LINE_ICD9_DGNS_CD_10", "LINE_ICD9_DGNS_CD_11", "LINE_ICD9_DGNS_CD_11",
                            "LINE_ICD9_DGNS_CD_13"]

inpatientFile = "../spark/data/raw/inpatient/inpatient.txt"
outpatientFile = "../spark/data/raw/outpatient/outpatient.txt"
carrierFile = "../spark/data/raw/carrier/carrier.txt"
beneficiaryFile = "../spark/data/raw/beneficiary/beneficiary.txt"
prescriptionFile = "../spark/data/raw/prescription/prescription.txt"
lovastatinFile = "../spark/data/lookup/lovastatin.txt"

class Panalgo:

    def read_csv(self, file):
        with open(file, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            return list(reader)

    def __init__(self):
        self.inpatient = self.read_csv(inpatientFile)
        self.outpatient = self.read_csv(outpatientFile)
        self.carrier = self.read_csv(carrierFile)
        self.beneficiary = self.read_csv(beneficiaryFile)
        self.prescription = self.read_csv(prescriptionFile)
        with open(lovastatinFile) as f:
            self.lovastatin = set(f.read().splitlines())

    def diabeticPatients(self):
        diabetic_patients = []
        for col_name in inpatientDiagnosisColNames:
            for patient in self.inpatient:
                if patient[PATIENT_DATE_FIELD].startswith('2009') and patient[col_name].startswith('250'):
                    diabetic_patient = {
                        PATIENT_ID_FIELD: patient[PATIENT_ID_FIELD],
                        DIABETES_INDEX_DATE: patient[PATIENT_DATE_FIELD],
                    }
                    diabetic_patients.append(diabetic_patient)
        for col_name in outpatientDiagnosisColNames:
            for patient in self.outpatient:
                if patient[PATIENT_DATE_FIELD].startswith('2009') and patient[col_name].startswith('250'):
                    diabetic_patient = {
                        PATIENT_ID_FIELD: patient[PATIENT_ID_FIELD],
                        DIABETES_INDEX_DATE: patient[PATIENT_DATE_FIELD],
                    }
                    diabetic_patients.append(diabetic_patient)
        for col_name in carrierDiagnosisColNames:
            for patient in self.carrier:
                if patient[PATIENT_DATE_FIELD].startswith('2009') and patient[col_name].startswith('250'):
                    diabetic_patient = {
                        PATIENT_ID_FIELD: patient[PATIENT_ID_FIELD],
                        DIABETES_INDEX_DATE: patient[PATIENT_DATE_FIELD],
                    }
                    diabetic_patients.append(diabetic_patient)
        return diabetic_patients

    def dateIndexediabeticPatients(self):
        diabetic_patients = [(dp[PATIENT_ID_FIELD], dp[DIABETES_INDEX_DATE]) for dp in self.diabeticPatients()]
        diabetic_patients.sort(key=lambda item: (item[0], item[1]))
        with_diabetes_index_date = []
        grouped = groupby(diabetic_patients, key=lambda item: item[0])
        for patient_id, group in grouped:
            grp = list(group)
            diabetic_patient = {
                PATIENT_ID_FIELD: patient_id,
                DIABETES_INDEX_DATE: grp[0][1],
            }
            with_diabetes_index_date.append(diabetic_patient)
        return with_diabetes_index_date

    def lovastatinPrescriptions(self):
        return [(p[PATIENT_ID_FIELD], p[PRESCRIPTION_DATE_FIELD]) for p in
                self.prescription if p[NDC_ID_FIELD] in self.lovastatin]

    def DiabeticPatientsWithLovastatinPrescriptions(self):
        lovastatin_patients = set()
        diabetic_patients = dict(
            [(dp[PATIENT_ID_FIELD], dp[DIABETES_INDEX_DATE]) for dp in self.dateIndexediabeticPatients()])
        prescriptions = self.lovastatinPrescriptions()
        for patient_id, prescription_date in prescriptions:
            if patient_id in diabetic_patients:
                diabetes_index_date = diabetic_patients[patient_id]
                diabetes_index_date_plus_year = str(int(diabetes_index_date[0:4]) + 1) + diabetes_index_date[4:]
                if prescription_date > diabetes_index_date and prescription_date < diabetes_index_date_plus_year:
                    lovastatin_patients.add(patient_id)
        return dict([(patient_id, diabetic_patients[patient_id]) for patient_id in lovastatin_patients])

    def retiredDiabeticPatientsWithLovastatinPrescriptions(self):
        retired_patients = {}
        lovastatin_patients = self.DiabeticPatientsWithLovastatinPrescriptions()
        beneficiaries = dict([(b[PATIENT_ID_FIELD], b['BENE_BIRTH_DT']) for b in self.beneficiary])
        for pid, date in lovastatin_patients.items():
            if pid in beneficiaries:
                diabetes_index_date = datetime.datetime.strptime(date, "%Y%m%d")
                birth_date = datetime.datetime.strptime(beneficiaries.get(pid), "%Y%m%d")
                delta = ((diabetes_index_date - birth_date).days) / 365
                if delta >= 65:
                    retired_patients[pid] = date
        return retired_patients

    def run(self):
        diabetic_patients = self.diabeticPatients()
        node1 = len(set([dp[PATIENT_ID_FIELD] for dp in diabetic_patients]))
        lovastatin_patients = self.DiabeticPatientsWithLovastatinPrescriptions()
        node2 = len(lovastatin_patients.keys())
        retired_patients = self.retiredDiabeticPatientsWithLovastatinPrescriptions()
        node3 = len(set(retired_patients.keys()))
        print(f"Section              Count")
        print(f"Node 1 (Diabetes):   {node1}")
        print(f"Node 2 (Lovastatin): {node2}")
        print(f"Node 3 (Age > 65):   {node3}")

if __name__ == "__main__":
    Panalgo().run()

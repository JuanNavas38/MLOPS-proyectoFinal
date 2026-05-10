import random
from locust import HttpUser, task, between

SAMPLE_PATIENTS = [
    {
        "time_in_hospital": 5, "num_lab_procedures": 45, "num_procedures": 2,
        "num_medications": 15, "number_outpatient": 0, "number_emergency": 1,
        "number_inpatient": 2, "number_diagnoses": 7, "age_encoded": 6,
        "admission_type_encoded": 1, "discharge_encoded": 1,
        "admission_source_encoded": 1, "insulin_encoded": 2, "change_encoded": 1,
        "diabetesmed_encoded": 1, "a1cresult_encoded": 0,
        "max_glu_serum_encoded": 0, "num_medications_log": 2.77,
        "service_utilization": 3,
    },
    {
        "time_in_hospital": 2, "num_lab_procedures": 30, "num_procedures": 0,
        "num_medications": 8, "number_outpatient": 1, "number_emergency": 0,
        "number_inpatient": 0, "number_diagnoses": 4, "age_encoded": 4,
        "admission_type_encoded": 2, "discharge_encoded": 1,
        "admission_source_encoded": 2, "insulin_encoded": 0, "change_encoded": 0,
        "diabetesmed_encoded": 1, "a1cresult_encoded": 1,
        "max_glu_serum_encoded": 0, "num_medications_log": 2.20,
        "service_utilization": 1,
    },
    {
        "time_in_hospital": 10, "num_lab_procedures": 70, "num_procedures": 5,
        "num_medications": 25, "number_outpatient": 2, "number_emergency": 3,
        "number_inpatient": 4, "number_diagnoses": 9, "age_encoded": 8,
        "admission_type_encoded": 1, "discharge_encoded": 3,
        "admission_source_encoded": 1, "insulin_encoded": 3, "change_encoded": 1,
        "diabetesmed_encoded": 1, "a1cresult_encoded": 2,
        "max_glu_serum_encoded": 1, "num_medications_log": 3.26,
        "service_utilization": 9,
    },
]


class DiabetesAPIUser(HttpUser):
    wait_time = between(0.5, 2)

    @task(10)
    def predict(self):
        payload = random.choice(SAMPLE_PATIENTS)
        with self.client.post(
            "/predict",
            json=payload,
            catch_response=True
        ) as response:
            if response.status_code == 200:
                response.success()
            else:
                response.failure(f"Status {response.status_code}: {response.text}")

    @task(2)
    def health_check(self):
        self.client.get("/health")

    @task(1)
    def model_info(self):
        self.client.get("/model-info")
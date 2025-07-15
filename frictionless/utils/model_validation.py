import json
import logging
import sys
from collections import defaultdict

log = logging.getLogger(__name__)


def validate_model(client, industry_id):
    """Checks the attributes for a customer model.

    Validates that a newly created customer's environment has been instantiated
    correctly from the selected industry model template.

    Performs validation across:
        1. KPIs — Verifies that KPIs were correctly generated.
        2. Functions — Validates function metadata per KPI.
        3. Roles — Ensures the appropriate roles are defined.
        4. Contexts — Lists and validates all metric contexts.
        5. Dictionaries — Checks dictionary metadata for all unique function codes.

    Args:
        client (OnboardingApiClient): Authenticated API client.
        industry_id (int): The industry/model template ID used to generate the customer.
    """
    try:
        # 1) Validate KPIs
        log.info("Validating KPIs...")
        kpi_dict = client.list_kpis(industry_id)
        log.debug(json.dumps(kpi_dict, indent=2))
        kpis = kpi_dict.get("data", {})
        if not kpis:
            log.warning("Validation Warning: No KPIs found in payload.")
        else:
            log.info(f"Found {len(kpis)} KPIs.")
            log.debug(json.dumps(kpis, indent=2))
            print("\n=== Available KPIs ===\n")
            kpi_map = {
                kpi["id"]: {
                    "functionName": kpi["functionName"],
                    "name": kpi["name"],
                    "metric_attributes": len(kpi.get("metric_attributes", [])),
                }
                for kpi in kpis
            }
            print(json.dumps(kpi_map, indent=2))

        # 2) Validate Functions (loop through each unqiue function name)
        log.info("Validating Functions...")
        functions = client.list_functions()
        log.debug(json.dumps(functions, indent=2))
        function_names = {kpi["functionName"] for kpi in kpis}

        # build a defualt mapping: function name -> list of its function blobs
        functions_by_name = defaultdict(list)
        for blob in functions:
            functions_by_name[blob["name"]].extend(blob.get("industry_function", []))

        # Define which fields to keep
        fields = [
            "id",
            "industry_function_map_id",
            "function_name",
            "industry_name",
            "subType",
            "name",
            "description",
            "useCaseId",
        ]

        function_info = [
            {field: item[field] for field in fields}
            for fn in function_names
            for item in functions_by_name.get(fn, [])
        ]
        print("\n=== Available Functions ===\n")
        print(json.dumps(function_info, indent=2))

        # 3) Validate Roles
        log.info("Validating Roles...")
        resp = client.get_industry_details(industry_id)
        log.debug(json.dumps(resp, indent=2))
        trimmed_roles = [
            {
                "id": r["id"],
                "levelName": r["levelName"],
                "role_display_name": r["role_display_name"],
            }
            for r in resp["roles"]
        ]
        print("\n=== Available Roles ===\n")
        print(json.dumps(trimmed_roles, indent=2))

        # 4) Validate Contexts
        log.info("Validating Contexts...")
        records = client.industry_metric_functions(industry_id)
        contexts = [r for r in records if r.get("typeName") == "Context"]
        log.info(f"Found {len(contexts)} Contexts.")
        log.debug(json.dumps(contexts, indent=2))

        context_summaries = [
            {
                "id": ctx["id"],
                "name": ctx.get("name"),
                "functionName": ctx["functionName"],
                "typeName": ctx["typeName"],
                "metric_attributes_count": len(ctx.get("metric_attributes", [])),
                "displayName": ctx.get("displayName"),
                "description": ctx.get("description"),
                "table": ctx.get("table"),
                "functionCode": ctx.get("functionCode"),
                "attribute": ctx.get("attribute"),
                "aggregation": ctx.get("aggregation"),
                "compute_frequency": ctx.get("compute_frequency"),
            }
            for ctx in contexts
        ]

        print(
            "\n=== Available Contexts ===\n" + json.dumps(context_summaries, indent=2)
        )

        # 5) Validate Dictionaries
        log.info("Validating Dictionaries...")
        f_codes = list(
            dict.fromkeys(ctx.get("functionCode") for ctx in context_summaries)
        )
        for f_code in f_codes:
            dictionary = client.get_dictionary(f_code)
            print(f"\n=== Dictionary for functionCode: {f_code} ===")

            trimmed = []
            for d in dictionary:
                trimmed.append(
                    {
                        "id": d["id"],
                        "name": d["name"],
                        "description": d.get("description"),
                        "functionName": d.get("functionName"),
                        "functionCode": d.get("functionCode"),
                        "sheetType": d.get("sheetType"),
                        "tableType": d.get("tableType"),
                        "entity_attributes": [
                            {
                                "id": attr["id"],
                                "name": attr["name"],
                                "description": attr.get("description"),
                                "dataType": attr.get("dataType"),
                            }
                            for attr in d.get("entity_attributes", [])
                        ],
                    }
                )
            print(json.dumps(trimmed, indent=2))
            log.debug(json.dumps(dictionary, indent=2))

    except Exception as e:
        log.error(f"An error occurred during model validation: {e}")
        sys.exit(1)

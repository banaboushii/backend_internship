import unittest
from datetime import datetime, timezone

from hubspot_dlt.source import ResourceSpec, _normalize


class SourceNormalizationTests(unittest.TestCase):
    def test_pipeline_keeps_top_level_business_fields(self):
        pipeline = _normalize(
            {
                "id": "sales",
                "label": "Sales pipeline",
                "displayOrder": 1,
                "stages": [],
            },
            ResourceSpec("pipelines", "/crm/v3/pipelines/deals", cursor_field=None),
        )

        self.assertEqual(pipeline["label"], "Sales pipeline")
        self.assertEqual(pipeline["displayOrder"], 1)
        self.assertEqual(pipeline["objectType"], "deals")
        self.assertIsInstance(pipeline["_version"], datetime)

    def test_owner_keeps_top_level_identity_fields(self):
        owner = _normalize(
            {
                "id": "42",
                "email": "owner@example.com",
                "firstName": "Casey",
                "lastName": "Lee",
                "updatedAt": "2026-10-10T12:00:00Z",
            },
            ResourceSpec("owners", "/crm/v3/owners"),
        )

        self.assertEqual(owner["email"], "owner@example.com")
        self.assertEqual(owner["firstName"], "Casey")
        self.assertEqual(
            owner["_version"], datetime(2026, 10, 10, 12, tzinfo=timezone.utc)
        )


if __name__ == "__main__":
    unittest.main()
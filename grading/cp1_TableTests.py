import requests, unittest, json
from MySupport import MySupport


class TableTests(unittest.TestCase):
    HOSTNAME = "localhost"
    PORT = 5000

    def suite():
        suite = unittest.TestSuite()
        suite.addTest(TableTests('test_no_tables'))
        suite.addTest(TableTests('test_create'))
        suite.addTest(TableTests('test_list_with_content'))
        suite.addTest(TableTests('test_delete'))
        suite.addTest(TableTests('test_getinfo'))
        suite.addTest(TableTests('test_cleanup'))

        return suite

    def test_no_tables(self):
        url = MySupport.url(self.HOSTNAME, self.PORT, "/api/tables")

        response = requests.get(url)
        self.assertEqual(response.status_code, 200)
        self.assertIsNotNone(response.content)

        jso = response.json()
        expected = {"tables": []}
        self.assertEqual(jso, expected)
        print("TEST_NO_TABLES PASSED!!!!!!!!!")

    def test_create(self):
        url = MySupport.url(self.HOSTNAME, self.PORT, "/api/tables")

        table_dict = {
            "name": "table1",
            "column_families": [
                {
                    "column_family_key": "key1",
                    "columns": ["column_key1", "column_key2"]
                },
                {
                    "column_family_key": "key2",
                    "columns": ["column_key3", "column_key4"]
                }
            ]
        }

        # create - success
        response = requests.post(url, json=table_dict)
        self.assertEqual(response.status_code, 200)
        self.assertFalse(response.content)

        # create - already exist
        response = requests.post(url, json=table_dict)
        self.assertEqual(response.status_code, 409)
        self.assertFalse(response.content)

        # create - not JSON
        response = requests.post(url, data="omgwtfbbq")
        self.assertEqual(response.status_code, 400)
        self.assertFalse(response.content)

        # create - success
        table_dict["name"] = "table2"
        response = requests.post(url, json=table_dict)
        self.assertEqual(response.status_code, 200)
        self.assertFalse(response.content)
        print("TEST_CREATE_TABLE PASSED!!!!!!!!!")

    def test_list_with_content(self):
        url = MySupport.url(self.HOSTNAME, self.PORT, "/api/tables")
        expected = {"tables": ["table1", "table2"]}

        response = requests.get(url)
        self.assertEqual(response.status_code, 200)
        self.assertIsNotNone(response.content)
        self.assertEqual(response.json(), expected)
        print ("TEST_LIST_WITH_CONTENT PASSED")

    def test_delete(self):
        # import pdb; pdb.set_trace()
        url = MySupport.url(self.HOSTNAME, self.PORT, "/api/tables")
        url_delete = url + "/table1"
        url_delete_nope = url + "/tablenope"
        expected_after = {"tables": ["table2"]}
        # url_delete = 'http://localhost:5000/api/tables/delete'
        # remove - success
        response = requests.delete(url_delete)
        self.assertEqual(response.status_code, 200)
        self.assertFalse(response.content)
        print("First test passed")

        # import pdb; pdb.set_trace()
        # remove - not exist
        response = requests.delete(url_delete_nope)
        self.assertEqual(response.status_code, 404)
        self.assertFalse(response.content)

        # get listing after delete
        response = requests.get(url)
        self.assertEqual(response.status_code, 200)
        self.assertIsNotNone(response.content)
        self.assertEqual(response.json(), expected_after)
        print("TEST_DELETE PASSED!!!!")

    def test_getinfo(self):
        url = MySupport.url(self.HOSTNAME, self.PORT, "/api/tables")
        url_table1 = url + "/table1"  # no longer exists
        url_table2 = url + "/table2"

        expected = {
            "name": "table2",
            "column_families": [
                {
                    "column_family_key": "key1",
                    "columns": ["column_key1", "column_key2"]
                },
                {
                    "column_family_key": "key2",
                    "columns": ["column_key3", "column_key4"]
                }
            ]
        }

        # getinfo - success
        response = requests.get(url_table2)
        self.assertEqual(response.status_code, 200)
        self.assertIsNotNone(response.content)
        self.assertEqual(response.json(), expected)

        # getinfo - not exist
        response = requests.get(url_table1)
        self.assertEqual(response.status_code, 404)
        self.assertFalse(response.content)

    def test_cleanup(self):
        url = MySupport.url(self.HOSTNAME, self.PORT, "/api/tables")
        url_delete = url + "/table2"
        expected_after = {"tables": []}

        # remove - success
        response = requests.delete(url_delete)
        self.assertEqual(response.status_code, 200)
        self.assertFalse(response.content)

        # get listing after delete
        response = requests.get(url)
        self.assertEqual(response.status_code, 200)
        self.assertIsNotNone(response.content)
        self.assertEqual(response.json(), expected_after)

from src.helpers import open_file, allocate

token_file_path = "./src/tokens/token_prod.txt"
token = open_file(token_file_path)

c_date = "2024-01-25T00:18:15.866316Z"
s_date = "2024-01-22T00:43:19.202368Z"  # last change
s_date2 = "2024-01-29T00:33:31.115444Z"  # last change

checklist = [
    ("public", "hippocampus", s_date),
    ("public", "thalamus", s_date),
    ("bbp-external", "seu", s_date2),
    ("bbp", "mouselight", c_date),
    ("public", "sscx", c_date)
]

for org, project, date_to_check in checklist:

    forge = allocate(org, project, True, token)
    query_nm = """
        SELECT ?id ?ua ?nm_id
        WHERE {
            ?id a NeuronMorphologyFeatureAnnotation ;
                _updatedAt ?ua ;
                _deprecated false ;
                hasTarget/hasSource ?nm_id
            FILTER(?ua < "%s"^^xsd:dateTime)
        }
    """ % date_to_check

    q_res = forge.sparql(query_nm, limit=10000)
    data = dict((r.id, r.nm_id) for r in q_res)
    print(org, project, len(data))
    if len(data) > 0:
        print(set(data.values()))  # nm that went wrong

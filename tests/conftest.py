def pytest_addoption(parser):
    parser.addoption(
        "--user", action="store", default="kanyesthaker", help="postgres username"
    )

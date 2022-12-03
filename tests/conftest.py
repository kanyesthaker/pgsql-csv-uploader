def pytest_addoption(parser):
    parser.addoption(
        "--user",
        action="store",
        default="kanyesthaker",
        help="Enter a PostgreSQL username to run the tests.",
    )

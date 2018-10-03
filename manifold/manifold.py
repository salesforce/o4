def main():
    import sys
    import .gatling
    sys.argv.insert(1, 'manifold')
    gatling.main()

def user_interface():
    print("\033[H\033[J", end="")
    print("   +----+")
    print("  /    /|")
    print(" +----+ |  +--------------------------------------------+")
    print(" |    | +   \ CubicalPy: enumeration of cubical surfaces \\")
    print(" |    |/     +--------------------------------------------+")
    print(" +----+\n")

    chunksize = 1000000
    print("By default, computing (connected) closed surfaces in the 5-cube.")
    response1 = input("Continue [C] or choose your own parameters [P]? ")
    if response1 == 'p':
        response2 = input("Closed surfaces [C] or surfaces with boundary [B]? ").lower()
        if response2 == 'b':
            surf_type = "surfaces with boundary"
            n = 4
            dbprefix = "b"
        elif response2 == 'c':
            surf_type = "closed surfaces"
            n = 5
            dbprefix = ""
        else:
            print("Invalid input. Exiting computation.")
            exit()
        response3 = input("Include disconnected surfaces? [Y/N] ")
        if response3 == 'y':
            surf_type += " (including disconnected)"
            if dbprefix == "b":
                dbprefix = "db"
            else:
                dbprefix = "dc"
        elif response3 == 'n':
            pass
        else:
            print("Invalid input. Exiting computation.")
            exit()
        response4 = input(f"Use default values (n = {n}, chunksize = 1000000)? [Y/N] ")
        if response4 == 'y':
            pass
        elif response4 == 'n':
            while True:
                try:
                    n = int(input("Please enter the dimension of the cube I^n: "))
                    if n > 2:
                        break
                    else:
                        print("The dimension must be greater than 2.")
                except ValueError:
                    pass
            if n > 6:
                response5 = input("WARNING!!! Large dimension: n > 6. Proceed at your own risk. Continue? [Y/N] ")
                if response5 == 'y':
                    pass
                else:
                    print("Cannot proceed without permission. Exiting Computation.")
                    exit()
            while True:
                try:
                    chunksize = int(input("Please select chunk size: "))
                    break
                except ValueError:
                    pass
        else:
            print("Invalid input. Exiting computation.")
            exit()
    elif response1 == 'c':
        surf_type = "closed surfaces"
        n = 5
        dbprefix = ""
    else:
        print("Invalid input. Exiting computation.")
        exit()

    if dbprefix == "b" or dbprefix == "db":
        if n >= 4:
            print(f"WARNING!!! Not checking connectedness of links (may include singular surfaces).")
    else:
        if n >= 6:
            print(f"WARNING!!! Not checking connectedness of links (may include singular surfaces).")
    return n, chunksize, dbprefix, surf_type
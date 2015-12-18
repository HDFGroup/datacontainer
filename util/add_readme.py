import h5py
import numpy as np
import argparse

def main():
  """ Updates given HDF5 with readme text provided in a text file.
      Text gets saved as attribute "readme" in the root group.
  """
  parser = argparse.ArgumentParser()
  parser.add_argument("--h5file", help="HDF5 File to be updated")
  parser.add_argument("--readme", help="Text file with readme content")
  
  args = parser.parse_args()

  if not args.h5file:
     print("No HDF5 given")
     return -1
  if not args.readme:
     print("No readme file given")
     return -1

  f = h5py.File(args.h5file, 'a')
 

  with open(args.readme, 'r', encoding="latin-1") as readme_file:
      text = readme_file.read()
      char_array = np.chararray((), itemsize=len(text))
      char_array[()] = text
      #print(char_array)
      f.attrs.create('readme', char_array)
  f.close()
  print("bye")

main()

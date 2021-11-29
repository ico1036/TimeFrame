#include <string>
#include <regex>

#include "TSystem.h"

void loopDirectory(std::string path, std::function<void(std::string)> callback, std::string ext = ".root")
{
   void* dirp = gSystem->OpenDirectory(path.c_str());

   const char* entry;
   TString str;

   while((entry = (char*)gSystem->GetDirEntry(dirp))) 
   {
      char* file = gSystem->ConcatFileName(path.c_str(), entry);

      str = entry;
      if(str.EndsWith(ext.c_str()))
      {
         callback(std::string(file));
      }

      delete file;
   }

   gSystem->FreeDirectory(dirp);
}

TChain* makeChain(std::string treeName, std::string path, std::string regexStr = ".*")
{
   TChain* chain = new TChain(treeName.c_str());

   loopDirectory(path, [&](std::string file)
   {
      const std::regex regex(regexStr);
      if(std::regex_match(file, regex))
      {
         std::cout << "Add file to chain: " << file << "\n";
         chain->Add(file.c_str());
      }
   });

   return chain;
}
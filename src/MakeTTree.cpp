#include "TFile.h"
#include "TTree.h"

#include "../include/TimeNS.h"

void MakeTTree()
{
    for(int j = 1; j < 4; j++)
    {
        TimeNS time = 0;
        int id;
        double x = 0, y = 0, z = 0;

        auto fileName = std::string("examples/Message_Of_Product_") + std::to_string(j) + ".root";
        TFile file(fileName.c_str(), "RECREATE", "");
        TTree tree("messages", "");

        tree.Branch("time", &time);
        tree.Branch("id", &id);
        tree.Branch("x", &x);
        tree.Branch("y", &y);
        tree.Branch("z", &z);

        std::random_device rd;
        std::mt19937 rng(rd());

        double lamda = 1.0 / T_Second;
        std::exponential_distribution<double> exp(lamda);

        std::uniform_real_distribution<double> xGenerator(0.0, 1.0);
        std::uniform_real_distribution<double> yGenerator(0.0, 10.0);
        std::uniform_real_distribution<double> zGenerator(0.0, 100.0);
    
        for(int i = 0; i < 100000; i++)
        {
            time += exp(rng);

            id = j;

            x = id + xGenerator(rng);
            y = id / yGenerator(rng);
            z = id * zGenerator(rng); 

            tree.Fill();
        }

        tree.Print();
    
        file.Write();
        file.Close();
    }

    return 0;
}
# Uniswap-MEV-Analysis
This MEV research is conducted for Uniswap Bounty #19. In this research, we dug into two famous MEV: Sandwich Attacks & JIT, and analyzed the prevalence of these two actions in Uniswap V3. <br>
## Research Paper
https://coinomo.notion.site/Uniswap-Bounty-19-34df5d8d69e54b2ba10f5b5799758d26
## Getting Started 
```python
git clone git@github.com:ZooWallet/Uniswap-MEV-Analysis.git
cd uniswap-mev-analysis
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```
## Download Data
To reproduce the results in the jupyter notebooks, you have to download the data first by running:
```
python3 getData.py
```
## Analysis Results 
The analysis is in two parts: Sandwich Attacks & JIT

### Sandwich Attacks
https://github.com/ZooWallet/Uniswap-MEV-Analysis/blob/main/sandwichAnalysis.ipynb
### JIT
https://github.com/ZooWallet/Uniswap-MEV-Analysis/blob/main/justInTimeAnalysis.ipynb

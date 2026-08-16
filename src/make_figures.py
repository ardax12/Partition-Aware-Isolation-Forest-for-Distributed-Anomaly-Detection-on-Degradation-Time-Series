"""CLI: rebuild every figure from the verified result files.
Usage (from project root):  python src/make_figures.py"""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.plots import make_detection_figures, make_scalability_figure

make_detection_figures()
make_scalability_figure()
print("Rebuilt figures/fig_auc.png, fig_f1.png, fig_scalability.png")

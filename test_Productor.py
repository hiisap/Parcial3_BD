"""
Your module description
"""
from Productor1 import generate
from Consumidor_upperBand import upper_band, procesar_datos
from Consumidor_lowerBand import lower_band
from unittest.mock import patch


def test_upperBand():
    hist_price = [4520.293688, 4610.680648, 4849.745016, 4499.697586, 4739.687573, 
                  4694.535353, 4437.616019, 4658.730548, 4581.472434, 4594.029405, 
                  4635.976184, 4599.08629, 4767.347706, 4531.933942, 4432.137717, 
                  4543.436718, 4468.804612, 4756.905427, 4712.210257, 4789.747155]
    
    result = upper_band(hist_price)
    
    assert result == 4865.848184756076

def test_lowerBand():
    hist_price = [4520.293688, 4610.680648, 4849.745016, 4499.697586, 4739.687573, 
                  4694.535353, 4437.616019, 4658.730548, 4581.472434, 4594.029405, 
                  4635.976184, 4599.08629, 4767.347706, 4531.933942, 4432.137717, 
                  4543.436718, 4468.804612, 4756.905427, 4712.210257, 4789.747155]
    
    result = lower_band(hist_price)
    
    assert result == 4376.559243043925
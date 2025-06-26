sub_metering_1: podlicznik energii nr 1 (w watogodzinach energii czynnej), dla:
-Kuchnia: zmywarka, piekarnik, kuchenka mikrofalowa.
sub_metering_2: podlicznik energii nr 2 (w watogodzinach energii czynnej), dla: 
-Pralnia: pralka, suszarka bębnowa, lodówka, oświetlenie.
9. sub_metering_3: podlicznik energii nr 3 (w watogodzinach energii czynnej), dla:
-Podgrzewacz wody i klimatyzator.

Wykresy trendów, średnich i wartości szczytowych:
![overview_metrics](https://github.com/user-attachments/assets/449340ca-5019-4f5b-a705-218397cae420)


Aby porównać różne metryki na jednej skali, każdą z nich znormalizowano metodą min–max:

<p align="center">
  <img
    src="https://latex.codecogs.com/png.latex?normalized\_value_h%20%3D%20\frac{value_h%20-%20\min_{h'}(value_{h'})}{\max_{h'}(value_{h'})%20-%20\min_{h'}(value_{h'})}"
    alt="normalized formula" />
</p>

gdzie:  
- <code>h</code> to godzina dnia (0–23),  
- <code>value<sub>h</sub></code> to agregowana wartość metryki w godzinie <code>h</code>,  
- <code>min<sub>h′</sub>(…)</code> i <code>max<sub>h′</sub>(…)</code> to odpowiednio najniższa i najwyższa wartość tej metryki spośród wszystkich godzin.

Wspólny wykres pozwala zobaczyć o której godzinie osiąga się szczyt danej miary, porównać kształty i rozkłady godzinowe różnych wielkości niezależnie od ich jednostek.
![image](https://github.com/user-attachments/assets/c3140c43-6b97-4433-98cb-bd592ec97a12)


Poniższy wykres grupuje dane według dni tygodnia (pon.–niedz.):

* **kWh / kVARh** – suma dobowych energii czynnej i biernej  
* **Sub 1-3 [Wh]** – łączna energia z trzech pod-liczników  
* **V / A** – średnie dzienne napięcie oraz natężenie

Wartości energii zostały przeliczone na Wh lub kWh, a napięcie i natężenie
pokazane jako średnia arytmetyczna. Dzięki temu łatwo widać,
który dzień tygodnia generuje najwyższe (lub najniższe) obciążenie.
![image](https://github.com/user-attachments/assets/0d4f84d1-3a2c-49bd-b478-0f6af4fcc5b3)

#### Dane wejściowe modelu LR dla zużycia aktywnego
* **Trend czasowy** – kolejny numer dnia (`t_idx`), łapie długofalowe zmiany.  
* **Sezon roczny** – `sin(dzień-roku)` i `cos(dzień-roku)` rozróżniają lato / zimę.  
* **Dzień tygodnia** – siedem flag (pn…nd) wskazujących konkretny dzień.

Razem 10 bardzo cech.

**Parametry treningu**

| element | wartość |
|---------|---------|
| Próbka  | grudzień 2006 → listopad 2010 (1 276 dni) |
| Podział | 80 % train / 20 % test |
| Model   | `LinearRegression` – Spark MLlib, `maxIter=50`, `regParam=0` |

\
Efekt:

| Metryka | Wartość |
|---------|---------|
| RMSE | **7.35 kWh** |
| R²   | **0.409** |

![image](https://github.com/user-attachments/assets/73da5c50-e026-45dc-a013-593b352df5a4)



sub_metering_1: podlicznik energii nr 1 (w watogodzinach energii czynnej), dla:
-Kuchnia: zmywarka, piekarnik, kuchenka mikrofalowa.
sub_metering_2: podlicznik energii nr 2 (w watogodzinach energii czynnej), dla: 
-Pralnia: pralka, suszarka bębnowa, lodówka, oświetlenie.
9. sub_metering_3: podlicznik energii nr 3 (w watogodzinach energii czynnej), dla:
-Podgrzewacz wody i klimatyzator.

Wykresy trendów, średnich i wartości szczytowych:
![overview_metrics](https://github.com/user-attachments/assets/449340ca-5019-4f5b-a705-218397cae420)


Aby porównać różne metryki (moc czynna, moc bierna, sub-meteringi, napięcie, natężenie) na jednej skali, każdą z nich znormalizowano metodą *min–max*:

```text
normalized_value_h = (value_h - min_{h'} value_{h'}) 
                     / (max_{h'} value_{h'} - min_{h'} value_{h'})

gdzie:
- \(h\) to godzina dnia (0–23),
- \(\text{value}_{h}\) to agregowana wartość danej metryki w godzinie \(h\),
- min/max to odpowiednio najmniejsza i największa wartość tej metryki wśród wszystkich godzin.

Wspólny wykres pozwala zobaczyć o której godzinie osiąga się szczyt danej miary, porównać kształty i rozkłady godzinowe różnych wielkości niezależnie od ich jednostek.
![hourly_combined_normalized](https://github.com/user-attachments/assets/a2720477-d36e-43ba-ad53-0a4839567e13)

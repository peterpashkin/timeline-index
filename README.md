# Timeline-Index

This is my implementation of the Timeline Index, a datastructure for the SAP HANA system, proposed in this paper:\
https://www.researchgate.net/publication/243961981_Timeline_Index_A_Unified_Data_Structure_for_Processing_Queries_on_Temporal_Data_in_SAP_HANA

It has some optimizations such as two-sided checkpoint lookup, multi-threading and my own implementation of the temporal max.\
Reference implementations of the original Timeline Index or some intermediate methods I benchmarked can be found in legacy_functions.cpp

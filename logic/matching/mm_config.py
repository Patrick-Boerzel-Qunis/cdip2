from typing import Tuple, List
from matrixmatcher import (
    MatrixField,
    Neighborhood,
    MatchMatrix,
    MatchCase,
)


def get_match_matrix_config(
    sliding_window_size: int = 21,
) -> Tuple[MatchMatrix, List[Neighborhood]]:
    """Specify matching algorithm and similarity thresholds for address consituents.

    Description:
        - The matches are done using the weights defined by "matchcase".
        - The similarity measure for each field is defined by field_**
        - Neighborhood defines the field using which we need to sort the matched data.
        - The match in the sorted neighborhood window of "sliding_window_size" is provided.

    Note:
        The sliding window has a pretty big (nonlinear) impact on the performance
        of indexing. So please try to keep it as small as possible.As a rule of
        thumb you should (almost) never choose a window size greater than 51,
        which has been big enough for all reference cases up until now.
        The sliding window size can be optimized by various standardizations.

    """
    # Matching with option of missing fields and weights set, which allow false positives.
    neighborhood = [
        Neighborhood(
            ["Ort", "PLZ", "Strasse", "Hausnummer", "Firmenname"],
            windowSize=sliding_window_size,
        ),
        Neighborhood(
            ["Firmenname", "Strasse", "Hausnummer", "PLZ", "Ort"],
            windowSize=sliding_window_size,
        ),
        Neighborhood(
            ["Handelsname", "PLZ", "Ort", "Strasse", "Hausnummer"],
            windowSize=sliding_window_size,
        ),
    ]

    field_handelsname = MatrixField(
        fieldName="Handelsname",
        method="worddiff",
        use_name_flags=True,
        cmp_legal_forms=True,
        performance_priority=5,
    )

    field_firmenname = MatrixField(
        fieldName="Firmenname",
        method="worddiff",
        use_name_flags=True,
        cmp_legal_forms=True,
        performance_priority=4,
    )

    field_strasse = MatrixField(
        fieldName="Strasse", method="worddiff_locality", performance_priority=3
    )
    field_hausnummer = MatrixField(
        fieldName="Hausnummer", method="housenumber", performance_priority=0
    )
    field_plz = MatrixField(fieldName="PLZ", method="identity", performance_priority=1)
    field_ort = MatrixField(
        fieldName="Ort", method="worddiff_locality", performance_priority=2
    )

    # Specify matching threshold
    # Optionen:
    # '+': Beide Felder müssen gefüllt sein.
    # '*': Beide Felder müssen entweder gefüllt oder leer sein
    # '-': Die Felder können unterschiedlich gefüllt oder nicht gefüllt sein.
    #                Handelsname, Firmenname  Strasse, Hausnummer, PLZ,    Ort
    C0 = MatchCase([0.94, 0.00, 0.94, 0.94, 1.00, 0.90], ["+", "+", "+", "+", "+", "+"])
    C0a = MatchCase(
        [0.00, 0.94, 0.94, 0.94, 1.00, 0.90], ["-", "+", "+", "+", "+", "+"]
    )

    C1 = MatchCase([0.90, 0.00, 0.94, 0.94, 1.00, 0.72], ["+", "+", "+", "+", "+", "+"])
    C1a = MatchCase(
        [0.00, 0.90, 0.94, 0.94, 1.00, 0.72], ["-", "+", "+", "+", "+", "+"]
    )

    C2 = MatchCase([0.86, 0.00, 0.94, 0.94, 1.00, 0.72], ["+", "+", "+", "+", "+", "+"])
    C2a = MatchCase(
        [0.00, 0.86, 0.94, 0.94, 1.00, 0.72], ["-", "+", "+", "+", "+", "+"]
    )

    match_matrix = MatchMatrix(
        [
            field_handelsname,
            field_firmenname,
            field_strasse,
            field_hausnummer,
            field_plz,
            field_ort,
        ],
        [C0, C0a, C1, C1a, C2, C2a],
    )

    return match_matrix, neighborhood


import time
import re
from functools import reduce
from collections import defaultdict
import multiprocessing
from functools import reduce

def lire_fichier(nom_fichier):
    """
    Lit le contenu texte d'un fichier et le renvoie sous forme de chaîne de caractères.
    """
    with open(nom_fichier, 'r', encoding='latin-1') as fichier:
        return fichier.read()

def decouper_texte_en_mots(texte):
    """
    Découpe le texte en mots en se basant sur une expression régulière,
    en filtrant les chaînes vides éventuelles.
    """
    mots = re.split(r'[\s.,;!?()\[\]{}<>|/\\"\':-]+', texte)
    mots = [mot.lower() for mot in mots if mot]  # on convertit tout en minuscule
    return mots

def segmenter_liste(liste, nb_segments):
    """
    Segmenter une liste (ici la liste des mots) en nb_segments morceaux
    aussi équilibrés que possible.
    """
    longueur = len(liste)
    taille_segment = max(1, longueur // nb_segments)
    
    segments = []
    for i in range(0, longueur, taille_segment):
        segments.append(liste[i:i+taille_segment])
    return segments

def compter_mots_monoproc(liste_mots):
    """
    Calcule le nombre d'occurrences de chaque mot de façon séquentielle (mono-processus).
    Renvoie (compteur_global, temps_calcul).
    
    - liste_mots : la liste complète des mots, déjà prétraitée.
    """
    start_time = time.time()
    
    compteur = defaultdict(int)
    for mot in liste_mots:
        compteur[mot] += 1
    
    end_time = time.time()
    return compteur, (end_time - start_time)


def split_into_chunks(liste_mots, num_chunks):
    """
    Splits the list of words (liste_mots) into num_chunks chunks.
    Returns a list of chunk strings, each containing words joined by spaces.
    """
    if num_chunks < 1:
        num_chunks = 1
    
    chunk_size = len(liste_mots) // num_chunks
    chunks = []
    start = 0
    
    for i in range(num_chunks):
        # For the last chunk, include all remaining words
        if i == num_chunks - 1:
            chunk_words = liste_mots[start:]
        else:
            chunk_words = liste_mots[start : start + chunk_size]
        
        start += chunk_size
        chunks.append(" ".join(chunk_words))
    
    return chunks

def map_function(text_chunk):
    """Fonction de map : retourne un dictionnaire des fréquences de mots pour le morceau de texte."""
    word_freq = {}
    for word in text_chunk.split():
        if word in word_freq:
            word_freq[word] += 1
        else:
            word_freq[word] = 1
    return word_freq
    
def reduce_function(counter_a, counter_b):
    """Reduce function: merges two word count dictionaries by summing counts."""
    for word, count in counter_b.items():
        if word in counter_a:
            counter_a[word] += count
        else:
            counter_a[word] = count
    return counter_a

def word_count_multiprocess(data_lines, processes=2):
    """
    Counts word occurrences using multiple processors.
    Measures and returns the time taken for each stage and the final counts.
    """
    timings = {}
    t_start_total = time.time()
    
    # Chunking
    t_start_chunk = time.time()
    chunks = split_into_chunks(data_lines, num_chunks=processes)
    t_end_chunk = time.time()
    timings['time_chunk'] = t_end_chunk - t_start_chunk
    
    # Mapping
    t_start_map = time.time()
    with multiprocessing.Pool(processes=processes) as pool:
        partial_counters = pool.map(map_function, chunks)
    t_end_map = time.time()
    timings['time_map'] = t_end_map - t_start_map
    
    # Reducing
    t_start_reduce = time.time()
    final_counts = reduce(reduce_function, partial_counters)
    t_end_reduce = time.time()
    timings['time_reduce'] = t_end_reduce - t_start_reduce
    
    t_end_total = time.time()
    timings['time_total'] = t_end_total - t_start_total
    
    return final_counts, timings

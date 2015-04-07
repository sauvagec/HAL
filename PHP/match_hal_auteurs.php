<?php
include_once("Param.php");
include_once("func/strings.php");
include_once("func/ref_externes.php");

/* transforme un tableau d'objets en tableau associatif 
 * return: le tableau transformé
 */
function objectToArray( $object ) {
	if( !is_object( $object ) && !is_array( $object ) ) {
		return $object;
	}
	if( is_object( $object ) ) {
		$object = get_object_vars( $object );
	}
	return array_map( 'objectToArray', $object );
}

/* récupère TOUTE la liste des auteurs de HAL en nb/$step requêtes
 * param: step : nombre d'auteurs renvoyés à chaque requête HAL
 * return: tableau associatif clé => id_auteur, hal_id 
 */
function get_auteurs($step = 9000) {
	global $db;

	/* inits */
	$publesia_auteurs = array();
	$hal_auteurs = array();
	$auteurs = array();
	/* on met le max pour pouvoir rentrer dans la boucle for */
	$total_hal_auteurs = PHP_INT_MAX;
	
	/* hashage des auteurs de la base */
	$sql = 'SELECT id_auteur, nom, prenom FROM auteurs WHERE hal_id IS NULL';
	$res = pg_query($db, $sql);
	$tmp = pg_fetch_all($res);
	
	foreach ($tmp as $auteur) {
		$cle = md5(removeAccents(sprintf("%s:%s", strtolower($auteur['nom']), strtolower($auteur['prenom']))));
		$publesia_auteurs[$cle] = $auteur['id_auteur'];
	}
	//print_r($publesia_auteurs);
	if (DEBUG) echo "publesia_auteurs:" . count($publesia_auteurs) . "\n";
	
	/* boucle d'appel à l'API HAL */
	for ($start = 0 ; $start < $total_hal_auteurs ; $start += $step) {
		/* remise à zéro de la limite de temps */
		set_time_limit(30);
		
		/* récupération des $step prochains auteurs */
		$url = sprintf("http://api.archives-ouvertes.fr/ref/author/?start=%d&rows=%d&fl=docid,idHal_i,lastName_s,firstName_s&wt=json", $start, $step);
		$tmp = objectToArray(json_decode(url($url)));
		/* la base interne est en latin1 => transcodage des valeurs HAL */
		array_walk_recursive($tmp, function(&$value, $key) {
			if (is_array($value) == false) {
				$value = iconv('UTF-8', 'ISO-8859-1', $value);
			}
		});
		//print_r($tmp);
		if (DEBUG) echo "auteurs_hal:" . count($tmp['response']['docs']) . "\n";

		/* recherche du nombre d'auteurs HAL une fois la première requête effectuée */
		if ($total_hal_auteurs == PHP_INT_MAX) {
			$total_hal_auteurs = intval($tmp['response']['numFound']);
			/*if (DEBUG) $total_hal_auteurs = 20000;*/
			if (DEBUG) echo "total_hal_auteurs:$total_hal_auteurs\n";
		}
	
		/* itération dans les auteurs HAL */
		foreach($tmp['response']['docs'] as $auteur) {
			$cle = md5(removeAccents(sprintf("%s:%s", @strtolower($auteur['lastName_s']), @strtolower($auteur['firstName_s']))));
			$id = $auteur['docid'];

			/* repérage des doublons */
			if (isset($hal_auteurs[$cle]))
				$id = false;

			/* enregistrement */
			$hal_auteurs[$cle] = $id;
		}
	}
	
	/* suppression de tous les doublons : valeur == false */
	if (DEBUG) echo "hal_auteurs avant filtrage des doublons:" . count($hal_auteurs) . "\n";
	$hal_auteurs = array_filter($hal_auteurs);
	if (DEBUG) echo "hal_auteurs après filtrage des doublons:" . count($hal_auteurs) . "\n";
		
	/* confrontation avec les auteurs de la base Publesia */
	foreach ($hal_auteurs as $cle => $auteur) {
		if (isset($publesia_auteurs[$cle])) {
			$auteurs[$cle] = array(
					'id_auteur' => $publesia_auteurs[$cle],
					'hal_id' => $hal_auteurs[$cle]);
		}
	}
	
	return $auteurs;
}

/* met à jour la table des auteurs transactonnellement
 * param: tableau associatif des auteurs
 * return: void
 */
function update_auteurs($auteurs) {
	global $db;
	
	pg_query($db, "BEGIN;");
	
	foreach ($auteurs as $key => $auteur) {
		$sql = sprintf("UPDATE auteurs SET hal_id = %d WHERE id_auteur = %d",
				$auteur['hal_id'],
				$auteur['id_auteur']);
		
		if (DEBUG) echo "sql: $sql\n";
		
		if (pg_query($db, $sql) == false) {
			pg_query($db, 'ROLLBACK;');
			die("Erreur SQL. Source: ".htmlentities(pg_last_error($err)));
		}
	}
	
	if (DEBUG) printf("RESULTAT: %d auteurs mis à jour depuis HAL\n", count($auteurs));
	pg_query($db, 'COMMIT;');
}

/* initialisations */
define ('DEBUG', true);
if (DEBUG) echo "<pre>MISE À JOUR DES AUTEURS DE LA BASE DEPUIS HAL\n";

if (($db = pg_connect(pg_connection_string())) === false) {
	die("Impossible de se connecter à la base $dbname sur $dbhost.");
}

/* récupération des correspondances id_auteur <=> hal_id */
$auteurs = get_auteurs();

/* correspondances */
if (DEBUG) print_r($auteurs);

/* mise à jour de la base */
update_auteurs($auteurs);
?>

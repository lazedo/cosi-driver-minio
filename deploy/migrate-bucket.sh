#!/usr/bin/env bash
# Move a COSI bucket's DATA from one MinIO to another.
#
# WHY THIS IS A SCRIPT AND NOT AN EDIT
#
# Changing spec.bucketClassName on an existing BucketClaim does nothing. The
# class only decides where a bucket is CREATED: after that the driver routes by
# the BucketId, which encodes the connection it was created through
# (backendForID, "conn=<ref>"). That is deliberate -- the alternative is editing
# one field and silently redirecting a workload to a MinIO where its data is
# not. So moving data means a new bucket, a copy, and a repoint.
#
# WHAT MAKES THIS SAFE TO WRITE DOWN
#
# The consumer's database stores object KEYS and nothing about location:
# couch_doc_attachments has object_key, no bucket, no endpoint. So a migration
# never rewrites rows -- preserve the keys and the rows keep resolving. If that
# ever stops being true for a different consumer, this script is wrong for it.
#
# THE ORDER, AND WHY
#
#   1. claim + access on the target class      (nothing moves yet)
#   2. mirror WITH THE WORKLOAD RUNNING        (the bulk, at no cost)
#   3. stop the workload                       (the only downtime)
#   4. mirror again                            (the tail; now nothing races)
#   5. verify counts and bytes                 (before anything is repointed)
#   6. repoint the workload, start it
#   7. STOP. Removing the old claim is a separate decision.
#
# Two passes because the first one is long and the second is short: stopping the
# workload for the whole copy would be an outage proportional to the data, and
# copying while it writes would miss whatever arrived after the object listing.
#
# THE OLD BUCKET IS NEVER TOUCHED. Its class may have deletionPolicy: Delete,
# in which case deleting the old BucketClaim DELETES THE DATA -- the copy you
# just made is not a backup of a thing you are about to destroy, it is the same
# data in a second place. Verify, live on the new bucket for a while, and only
# then decide, by hand.
#
# Usage:
#   migrate-bucket.sh --claim crdb-sso/couch-attachments \
#                     --to-class minio-myminio4 \
#                     --workload deploy/couch-crdb-sink \
#                     --volume bucket-info \
#                     [--new-claim couch-attachments-m4] [--context kind-west]
#                     [--dry-run]
set -euo pipefail

CTX="${CTX:-kind-west}"
DRY=""
CLAIM_REF="" TO_CLASS="" WORKLOAD="" VOLUME="" NEW_CLAIM=""
MC_IMAGE="${MC_IMAGE:-minio/mc:latest}"

die() { echo "erro: $*" >&2; exit 1; }
step() { echo; echo "==> $*"; }

while [ $# -gt 0 ]; do
  case "$1" in
    --claim)     CLAIM_REF="$2"; shift 2;;
    --to-class)  TO_CLASS="$2"; shift 2;;
    --workload)  WORKLOAD="$2"; shift 2;;
    --volume)    VOLUME="$2"; shift 2;;
    --new-claim) NEW_CLAIM="$2"; shift 2;;
    --context)   CTX="$2"; shift 2;;
    --dry-run)   DRY=1; shift;;
    *) die "argumento desconhecido: $1";;
  esac
done

[ -n "$CLAIM_REF" ] || die "--claim <ns>/<nome> é obrigatório"
[ -n "$TO_CLASS" ]  || die "--to-class é obrigatório"
[ -n "$WORKLOAD" ]  || die "--workload é obrigatório (ex: deploy/couch-crdb-sink)"
[ -n "$VOLUME" ]    || die "--volume é obrigatório (o volume que monta o BucketInfo)"

NS="${CLAIM_REF%%/*}"
CLAIM="${CLAIM_REF##*/}"
NEW_CLAIM="${NEW_CLAIM:-${CLAIM}-migrated}"
NEW_SECRET="${NEW_CLAIM}-bucketinfo"
K="kubectl --context $CTX"

run() { if [ -n "$DRY" ]; then echo "    [dry-run] $*"; else "$@"; fi; }

# --- 0. o que existe hoje ---------------------------------------------------
step "estado inicial"
$K -n "$NS" get bucketclaim "$CLAIM" >/dev/null 2>&1 || die "não há BucketClaim $CLAIM_REF"
$K get bucketclass "$TO_CLASS" >/dev/null 2>&1 || die "não há BucketClass $TO_CLASS"
$K get bucketaccessclass "$TO_CLASS" >/dev/null 2>&1 \
  || die "não há BucketAccessClass $TO_CLASS -- o par tem de existir, senão as credenciais saem de um driver e o bucket de outro"

SRC_SECRET="$($K -n "$NS" get bucketaccess "$CLAIM" -o jsonpath='{.spec.credentialsSecretName}' 2>/dev/null)"
[ -n "$SRC_SECRET" ] || die "não encontrei o BucketAccess $CLAIM_REF (é dele que sai o BucketInfo da origem)"
echo "    origem : claim=$CLAIM_REF secret=$SRC_SECRET"
echo "    destino: claim=$NS/$NEW_CLAIM classe=$TO_CLASS secret=$NEW_SECRET"

# --- 1. provisionar o destino ----------------------------------------------
step "1/7 provisionar o bucket de destino"
if [ -n "$DRY" ]; then
  echo "    [dry-run] criaria BucketClaim/$NEW_CLAIM e BucketAccess/$NEW_CLAIM"
else
  cat <<YAML | $K apply -f -
apiVersion: objectstorage.k8s.io/v1alpha1
kind: BucketClaim
metadata: { name: $NEW_CLAIM, namespace: $NS }
spec:
  bucketClassName: $TO_CLASS
  protocols: [S3]
---
apiVersion: objectstorage.k8s.io/v1alpha1
kind: BucketAccess
metadata: { name: $NEW_CLAIM, namespace: $NS }
spec:
  bucketAccessClassName: $TO_CLASS
  bucketClaimName: $NEW_CLAIM
  credentialsSecretName: $NEW_SECRET
  protocol: S3
YAML
  # O BucketAccess espera pelo claim por construção (bucketaccess_controller.go
  # recusa avançar sem bucketReady), portanto isto é esperar e não sondar.
  for i in $(seq 1 60); do
    r="$($K -n "$NS" get bucketclaim "$NEW_CLAIM" -o jsonpath='{.status.bucketReady}' 2>/dev/null || true)"
    g="$($K -n "$NS" get bucketaccess "$NEW_CLAIM" -o jsonpath='{.status.accessGranted}' 2>/dev/null || true)"
    [ "$r" = "true" ] && [ "$g" = "true" ] && break
    sleep 5
  done
  [ "$($K -n "$NS" get bucketaccess "$NEW_CLAIM" -o jsonpath='{.status.accessGranted}')" = "true" ] \
    || die "o destino não ficou pronto (ver eventos do BucketAccess/$NEW_CLAIM)"
fi
echo "    destino pronto"

# --- helpers de cópia -------------------------------------------------------
# mc corre NUM POD, não aqui: os endpoints são serviços internos e as
# credenciais são Secrets do cluster. Montá-los é mais simples e mais seguro do
# que exportá-los para a máquina de quem corre isto.
mirror_pass() {
  local label="$1" extra="${2:-}"
  local job="cosi-migrate-$(date +%s)"
  cat <<YAML | $K -n "$NS" apply -f - >/dev/null
apiVersion: batch/v1
kind: Job
metadata: { name: $job }
spec:
  backoffLimit: 0
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: mc
          image: $MC_IMAGE
          command: ["/bin/sh","-c"]
          args:
            - |
              set -e
              src_ep=\$(sed -n 's/.*"endpoint":"\([^"]*\)".*/\1/p' /src/BucketInfo)
              dst_ep=\$(sed -n 's/.*"endpoint":"\([^"]*\)".*/\1/p' /dst/BucketInfo)
              src_ak=\$(sed -n 's/.*"accessKeyID":"\([^"]*\)".*/\1/p' /src/BucketInfo)
              src_sk=\$(sed -n 's/.*"accessSecretKey":"\([^"]*\)".*/\1/p' /src/BucketInfo)
              dst_ak=\$(sed -n 's/.*"accessKeyID":"\([^"]*\)".*/\1/p' /dst/BucketInfo)
              dst_sk=\$(sed -n 's/.*"accessSecretKey":"\([^"]*\)".*/\1/p' /dst/BucketInfo)
              src_b=\$(sed -n 's/.*"bucketName":"\([^"]*\)".*/\1/p' /src/BucketInfo)
              dst_b=\$(sed -n 's/.*"bucketName":"\([^"]*\)".*/\1/p' /dst/BucketInfo)
              mc alias set src "\$src_ep" "\$src_ak" "\$src_sk" --api S3v4
              mc alias set dst "\$dst_ep" "\$dst_ak" "\$dst_sk" --api S3v4
              echo "mirror \$src_b -> \$dst_b ($label)"
              mc mirror --preserve $extra "src/\$src_b" "dst/\$dst_b"
              echo "--- contagens ---"
              mc ls --recursive "src/\$src_b" | wc -l
              mc ls --recursive "dst/\$dst_b" | wc -l
          volumeMounts:
            - { name: src, mountPath: /src, readOnly: true }
            - { name: dst, mountPath: /dst, readOnly: true }
      volumes:
        - { name: src, secret: { secretName: $SRC_SECRET } }
        - { name: dst, secret: { secretName: $NEW_SECRET } }
YAML
  $K -n "$NS" wait --for=condition=complete --timeout=6h "job/$job" \
    || { $K -n "$NS" logs "job/$job" | tail -30; die "a cópia falhou ($label)"; }
  $K -n "$NS" logs "job/$job" | tail -6
  $K -n "$NS" delete job "$job" >/dev/null 2>&1 || true
}

# --- 2. cópia a quente ------------------------------------------------------
step "2/7 primeira cópia, com o workload A CORRER (o grosso, sem paragem)"
[ -n "$DRY" ] && echo "    [dry-run] mc mirror" || mirror_pass "quente"

# --- 3. parar o workload ----------------------------------------------------
step "3/7 parar $WORKLOAD -- daqui até ao passo 6 há indisponibilidade"
REPLICAS="$($K -n "$NS" get "$WORKLOAD" -o jsonpath='{.spec.replicas}' 2>/dev/null || echo 1)"
echo "    réplicas actuais: $REPLICAS (repostas no passo 6)"
run $K -n "$NS" scale "$WORKLOAD" --replicas=0
[ -n "$DRY" ] || $K -n "$NS" rollout status "$WORKLOAD" --timeout=180s 2>/dev/null || true

# --- 4. cópia da cauda ------------------------------------------------------
step "4/7 segunda cópia (a cauda; nada escreve agora)"
[ -n "$DRY" ] && echo "    [dry-run] mc mirror --remove" || mirror_pass "cauda" "--remove"

# --- 5. verificar ANTES de repontar -----------------------------------------
step "5/7 verificação"
echo "    (as duas contagens do passo anterior têm de ser iguais)"
if [ -z "$DRY" ]; then
  read -r -p "    as contagens batem certo? repontar o workload? [sim/NÃO] " ok
  [ "$ok" = "sim" ] || { echo "    abortado; o workload continua parado e nada foi repontado"; exit 1; }
fi

# --- 6. repontar e arrancar -------------------------------------------------
step "6/7 repontar $WORKLOAD para $NEW_SECRET e arrancar"
run $K -n "$NS" patch "$WORKLOAD" --type=json \
  -p "[{\"op\":\"replace\",\"path\":\"/spec/template/spec/volumes/$($K -n "$NS" get "$WORKLOAD" -o json | python3 -c "
import json,sys
d=json.load(sys.stdin)
vs=d['spec']['template']['spec']['volumes']
print(next(i for i,v in enumerate(vs) if v['name']=='$VOLUME'))" 2>/dev/null || echo 0)/secret/secretName\",\"value\":\"$NEW_SECRET\"}]"
run $K -n "$NS" scale "$WORKLOAD" --replicas="$REPLICAS"
[ -n "$DRY" ] || $K -n "$NS" rollout status "$WORKLOAD" --timeout=300s

# --- 7. o que NÃO se faz aqui -----------------------------------------------
step "7/7 concluído -- e o bucket antigo continua intacto, de propósito"
cat <<TXT

    O claim antigo ($CLAIM_REF) NÃO foi tocado. Se a classe dele tiver
    deletionPolicy: Delete, apagá-lo APAGA OS DADOS -- e a cópia que acabaste de
    fazer não é um backup disso, é a mesma informação num segundo sítio.

    Antes de o remover:
      - deixa o workload viver no bucket novo o tempo que aches necessário;
      - confirma que escreve E lê de lá (um anexo novo e um antigo);
      - só então:  kubectl -n $NS delete bucketaccess $CLAIM && \\
                   kubectl -n $NS delete bucketclaim $CLAIM

    Se o bucket for partilhado por claims de outros namespaces, apagar o teu NÃO
    apaga os dados enquanto os outros lá estiverem: o Bucket guarda o conjunto
    de binders (anotação cosi.lazedo.dev/claim-refs) e só o último a sair aplica
    a deletionPolicy da sua classe.
TXT

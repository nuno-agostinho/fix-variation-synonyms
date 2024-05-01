#!/usr/bin/env nextflow
nextflow.enable.dsl=2

params.database = 'homo_sapiens_variation_112_38'
params.host     = null
params.port     = null
params.user     = null
params.pass     = null

log.info "\nFix variation synonyms"
log.info "========================"
for (a in params) {
  // print param
  log.info "  ${a.getKey().padRight(8)} : ${a.getValue()}"
  // raise error if param is null
  if (!a.getValue()) exit 1, "ERROR: parameter --${a.getKey()} not defined"  
}
log.info ""

process fix_var_synonyms {
    input:
      path json

    output:
     path 'changed_data.out', emit: changed
     path '*_updates.sql', emit: sql_updates
     path '*_inserts.sql', emit: sql_inserts

    script:
    def pass = params.pass ? "--pass ${params.pass}" : ""
    """
    fix_variation_synonyms.py \\
      --json ${json} \\
      --host ${params.host} \\
      --port ${params.port} \\
      --user ${params.user} \\
      ${pass} \\
      --database ${params.database}
    """
}

workflow {
    data = channel.fromPath('input/*')
    out = fix_var_synonyms(data)

    out.changed.collectFile(storeDir: 'outdir', sort: true)
    out.sql_inserts.collectFile(storeDir: 'outdir', sort: true)
    out.sql_updates.collectFile(storeDir: 'outdir', sort: true)
}

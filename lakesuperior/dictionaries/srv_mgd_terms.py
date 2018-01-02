from lakesuperior.dictionaries.namespaces import ns_collection as nsc

srv_mgd_subjects = {
    nsc['fcsystem'].root,
}

srv_mgd_predicates = {
    nsc['fcrepo'].created,
    nsc['fcrepo'].createdBy,
    nsc['fcrepo'].hasFixityService,
    nsc['fcrepo'].hasParent,
    nsc['fcrepo'].lastModified,
    nsc['fcrepo'].lastModifiedBy,
    nsc['fcrepo'].writable,
    nsc['iana'].describedBy,
    nsc['ldp'].contains,
    nsc['premis'].hasMessageDigest,
    nsc['premis'].hasSize,
}

srv_mgd_types = {
    nsc['fcrepo'].Binary,
    nsc['fcrepo'].Container,
    nsc['fcrepo'].Pairtree,
    nsc['fcrepo'].Resource,
    nsc['fcrepo'].Version,
    nsc['ldp'].BasicContainer,
    nsc['ldp'].Container,
    nsc['ldp'].DirectContainer,
    nsc['ldp'].IndirectContainer,
    nsc['ldp'].NonRDFSource,
    nsc['ldp'].RDFSource,
    nsc['ldp'].Resource,
}


